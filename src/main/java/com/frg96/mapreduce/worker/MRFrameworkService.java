package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.*;
import com.frg96.mapreduce.File;
import com.frg96.mapreduce.api.Mapper;
import com.frg96.mapreduce.api.Reducer;
import com.frg96.mapreduce.api.TaskRegistry;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Implementation of the MRFrameworkGrpc.MRFrameworkImplBase interface.
 * Handles RPC calls from the client.
 */
public class MRFrameworkService extends MRFrameworkGrpc.MRFrameworkImplBase {
    private static final Logger logger = Logger.getLogger(MRFrameworkService.class.getName());

    private final String address;

    private final AtomicInteger mapperCounter;
    private final AtomicInteger reducerCounter;

    public MRFrameworkService(String address) {
        this.address = address;
        this.mapperCounter = new AtomicInteger(0);
        this.reducerCounter = new AtomicInteger(0);
    }

    /**
     * Server side implementation of the mapper() rpc method.
     * @param request MapperInput object passed by the client
     * @param responseObserver StreamObserver object used to send back the MapperOutput
     */
    @Override
    public void mapper(MapperInput request, StreamObserver<MapperOutput> responseObserver) {
        try {
            int currentMapperCounter = mapperCounter.incrementAndGet();
            logger.info("Started mapper() call # " + currentMapperCounter);

            long startTime = System.nanoTime();

            String appId = request.getAppId();
            int numPartitions = request.getNumPartitions();
            String outputDir = request.getOutputDir();

            List<Path> intermediateFiles = new ArrayList<>();
            String workerAddress = address.replace(":", "-");
            for (int i = 0; i < numPartitions; i++) {
                Path intermediateFile = Path.of(
                        outputDir,
                        "intermediate",
                        "mapper",
                        "part-" + i + "_work-" + workerAddress + "_map-" + currentMapperCounter
                );

                intermediateFiles.add(intermediateFile);
            }

            // Create a new MapperContextImpl object
            MapperContextImpl mapperContext = new MapperContextImpl(numPartitions, intermediateFiles);

            // Create a new user-defined Mapper object using TaskRegistry
            Mapper userMapper = TaskRegistry.createMapper(appId);

            // Calling the user-defined mapper for each line in shard
            for (ShardSplit shardSplit : request.getInputSplitsList()) {

                long start = shardSplit.getStartPos();
                long end = shardSplit.getEndPos();
                int length = Math.toIntExact(end - start);

                byte[] buffer = new byte[length];

                try (RandomAccessFile file = new RandomAccessFile(shardSplit.getFileName(), "r")) {
                    file.seek(start);
                    file.readFully(buffer);
                } catch (IOException e) {
                    logger.severe("Failed to read file: " + shardSplit.getFileName());
                    throw new RuntimeException(e);
                }

                try (BufferedReader reader = new BufferedReader(
                        new StringReader(new String(buffer, StandardCharsets.UTF_8))
                )) {
                    String line;

                    while ((line = reader.readLine()) != null) {
                        // Calling the user-defined map function for each line
                        userMapper.map(line, mapperContext);
                    }
                } catch (IOException e) {
                    logger.severe("Failed to read line from file: " + shardSplit.getFileName());
                    throw new RuntimeException(e);
                }
            }

            // Flushing the emitted key value pairs to intermediate files
            mapperContext.flush();

            // Set MapperOutput
            MapperOutput.Builder response = MapperOutput.newBuilder();

            for (Path file : intermediateFiles) {
                response.addFiles(File.newBuilder().setFileName(file.toString()).build());
            }

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

            long endTime = System.nanoTime();
            logger.info("Finished mapper() call # " + currentMapperCounter + " in " + (endTime - startTime) / 1_000_000 + " ms");


        } catch(Exception e) {
            logger.severe("Failed to execute mapper() call");
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * Server side implementation of the reducer() rpc method.
     * @param request ReducerInput object passed by the client
     * @param responseObserver StreamObserver object used to send back the ReducerOutput
     */
    @Override
    public void reducer(ReducerInput request, StreamObserver<ReducerOutput> responseObserver) {
        try {
            int currentReducerCounter = reducerCounter.incrementAndGet();
            logger.info("Started reducer() call # " + currentReducerCounter);

            long startTime = System.nanoTime();

            String appId = request.getAppId();
            int partitionId = request.getPartitionId();
            String outputDir = request.getOutputDir();

            String workerAddress = address.replace(":", "-");

            Path reducerOutputFile = Path.of(
                outputDir,
                "intermediate",
                "reducer",
                "part-" + partitionId + "_work-" + workerAddress + "_reduce-" + currentReducerCounter
            );

            // Create a new ReduceContextImpl object
            ReducerContextImpl reducerContext = new ReducerContextImpl(reducerOutputFile);

            // Create a new user-defined Reducer object using TaskRegistry
            Reducer userReducer = TaskRegistry.createReducer(appId);

            // reduce operation
            IntermediateMerger.mergeAndReduce(userReducer, request.getFilesList(), reducerContext);


            // Flushing the emitted key value pairs to intermediate files
            reducerContext.flush();

            // Set ReducerOutput
            ReducerOutput.Builder response = ReducerOutput.newBuilder();

            response.setFile(File.newBuilder()
                    .setFileName(reducerOutputFile.toString())
                    .build()
            );

            responseObserver.onNext(response.build());
            responseObserver.onCompleted();

            long endTime = System.nanoTime();
            logger.info("Finished reducer() call # " + currentReducerCounter + " in " + (endTime - startTime) / 1_000_000 + " ms");


        }  catch(Exception e) {
            logger.severe("Failed to execute reducer() call");
            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage())
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}
