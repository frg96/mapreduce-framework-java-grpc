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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Worker-side gRPC service that executes mapper and reducer tasks.
 *
 * <p>For each request, the service resolves the user-defined task through
 * {@link TaskRegistry} using the request's application ID. Mapper requests
 * read their assigned input byte ranges and write partitioned intermediate
 * files. Reducer requests merge intermediate files and write one partition
 * result.</p>
 *
 * <p>Input and output paths refer to the worker's filesystem. Consequently,
 * the client and all workers must have access to the same paths. Execution
 * failures are returned to the caller as gRPC
 * {@link io.grpc.Status.Code#INTERNAL} errors.</p>
 *
 * <p>The gRPC server may invoke this service concurrently. Per-request state
 * is kept local, while atomic counters provide unique task numbers for
 * worker-generated file names.</p>
 */
public class MRFrameworkService extends MRFrameworkGrpc.MRFrameworkImplBase {
    private static final Logger LOGGER = Logger.getLogger(MRFrameworkService.class.getName());

    private final String address;

    private final AtomicInteger mapperCounter;
    private final AtomicInteger reducerCounter;

    /**
     * Creates a worker service associated with a server address.
     *
     * @param address worker address used to identify generated output files
     */
    public MRFrameworkService(String address) {
        this.address = address;
        this.mapperCounter = new AtomicInteger(0);
        this.reducerCounter = new AtomicInteger(0);
    }

    /**
     * Executes a mapper task.
     *
     * <p>The request identifies the registered application, input byte ranges,
     * partition count, and output directory. Each input range is read, passed
     * line by line to the application's mapper, and flushed into one sorted
     * intermediate file per partition.</p>
     *
     * <p>Successful execution sends one {@link MapperOutput} and completes the
     * response stream. Failures are reported through
     * {@link StreamObserver#onError(Throwable)}.</p>
     *
     * @param request mapper task and input-shard description
     * @param responseObserver observer that receives the mapper result or error
     */
    @Override
    public void mapper(MapperInput request, StreamObserver<MapperOutput> responseObserver) {
        int currentMapperCounter = mapperCounter.incrementAndGet();
        try {
            LOGGER.log(
                    Level.INFO,
                    "Mapper task {0} started: app={1}, splits={2}, partitions={3}",
                    new Object[]{
                            currentMapperCounter,
                            request.getAppId(),
                            request.getInputSplitsCount(),
                            request.getNumPartitions()
                    }
            );

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
                    throw new IOException(
                            "Failed to read mapper input: " + shardSplit.getFileName(),
                            e
                    );
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
                    throw new IOException(
                            "Failed to read line from file: " + shardSplit.getFileName(),
                            e
                    );
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
            LOGGER.log(
                    Level.INFO,
                    "Mapper task {0} completed in {1} ms",
                    new Object[]{currentMapperCounter, (endTime - startTime) / 1_000_000}
            );


        } catch(Exception e) {
            LOGGER.log(
                    Level.SEVERE,
                    "Mapper task " + currentMapperCounter + " failed for application " + request.getAppId(),
                    e
            );

            String description = e.getMessage() != null
                    ? e.getMessage()
                    : e.getClass().getSimpleName();

            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(description)
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }

    /**
     * Executes a reducer task.
     *
     * <p>The request identifies the registered application, reduce partition,
     * intermediate input files, and output directory. Sorted intermediate files
     * are merged by key and passed to the application's reducer. The emitted
     * records are flushed to a partition output file.</p>
     *
     * <p>Successful execution sends one {@link ReducerOutput} and completes the
     * response stream. Failures are reported through
     * {@link StreamObserver#onError(Throwable)}.</p>
     *
     * @param request reducer task and intermediate-file description
     * @param responseObserver observer that receives the reducer result or error
     */
    @Override
    public void reducer(ReducerInput request, StreamObserver<ReducerOutput> responseObserver) {
        int currentReducerCounter = reducerCounter.incrementAndGet();
        try {
            LOGGER.log(
                    Level.INFO,
                    "Reducer task {0} started: app={1}, partition={2}, files={3}",
                    new Object[]{
                            currentReducerCounter,
                            request.getAppId(),
                            request.getPartitionId(),
                            request.getFilesCount()
                    }
            );

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
            LOGGER.log(
                    Level.INFO,
                    "Reducer task {0} completed in {1} ms",
                    new Object[]{currentReducerCounter, (endTime - startTime) / 1_000_000}
            );


        }  catch(Exception e) {
            LOGGER.log(
                    Level.SEVERE,
                    "Reducer task " + currentReducerCounter + " failed for application " + request.getAppId(),
                    e
            );

            String description = e.getMessage() != null
                    ? e.getMessage()
                    : e.getClass().getSimpleName();

            responseObserver.onError(
                    Status.INTERNAL
                            .withDescription(description)
                            .withCause(e)
                            .asRuntimeException()
            );
        }
    }
}
