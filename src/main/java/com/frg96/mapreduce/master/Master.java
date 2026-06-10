package com.frg96.mapreduce.master;

import com.frg96.mapreduce.*;
import com.frg96.mapreduce.core.FileShard;
import com.frg96.mapreduce.core.MapReduceSpec;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;

public class Master {
    private final MapReduceSpec spec;
    private final List<WorkerInfo> workerInfos = new ArrayList<>();
    private final List<ShardInfo> shardInfos = new ArrayList<>();
    private final List<PartitionInfo> partitionInfos = new ArrayList<>();

    private final BlockingQueue<Integer> readyShards = new LinkedBlockingQueue<>();
    private final BlockingQueue<Integer> readyPartitions = new LinkedBlockingQueue<>();

    private static final int POISON_PILL = -1;

    public Master(MapReduceSpec spec, List<FileShard> fileShards) {
        this.spec = spec;

        for(int i = 0; i < spec.numWorkers(); i++) {
            workerInfos.add(new WorkerInfo(i, spec.workerAddresses().get(i)));
        }

        for(int i = 0; i < fileShards.size(); i++) {
            shardInfos.add(new ShardInfo(i, fileShards.get(i)));
        }

        for(int i = 0; i < spec.numPartitions(); i++) {
            partitionInfos.add(new PartitionInfo(i));
        }
    }

    public boolean run() {
        try{
            if(!prepareFinalOutputDirectory(spec.outputDir()))
                return false;

            runMapPhase();

//            runReducePhase();

            return true;
        } finally {
            workerInfos.forEach(WorkerInfo::shutdownClient);
        }
    }

    /**
     * Prepares the final output directory by deleting all files in it.
     * @param outputDir the output directory path
     * @return true if the final output directory was prepared successfully, false otherwise
     */
    private boolean prepareFinalOutputDirectory(String outputDir) {
        Path finalOutputDir = Path.of(outputDir, "final");

        try {
            if(Files.exists(finalOutputDir)) {
                try(var paths = Files.walk(finalOutputDir)) {
                    paths.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try{
                                    Files.delete(path);
                                } catch (IOException e) {
                                    System.err.println("Failed to delete file: " + path);
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }

            Files.createDirectories(finalOutputDir);

            return true;

        } catch (IOException e) {
            System.err.println("Failed to prepare final output directory: " + finalOutputDir);
            return false;
        }
    }

    private void runMapPhase() {
        CountDownLatch latch = new CountDownLatch(shardInfos.size());

        for(ShardInfo shardInfo: shardInfos) {
            readyShards.offer(shardInfo.getId());
        }

        try(ExecutorService executor = Executors.newFixedThreadPool(workerInfos.size())) {
            for(WorkerInfo workerInfo: workerInfos) {
                executor.submit(() -> mapperTask(workerInfo, latch));
            }

            await(latch);

            for(int i = 0; i < workerInfos.size(); i++) {
                readyShards.offer(POISON_PILL);
            }
        } // executor will auto close
    }

    private void mapperTask(WorkerInfo workerInfo, CountDownLatch latch) {
        while(!Thread.currentThread().isInterrupted()) {
            int shardId;

            try {
                shardId = readyShards.take(); // blocking
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if(shardId == POISON_PILL) {
                return;
            }

            ShardInfo shard = shardInfos.get(shardId);
            shard.setStatus(TaskStatus.IN_PROGRESS);
            workerInfo.setWorkerStatus(WorkerStatus.BUSY);

            try {
                MapperInput input = buildMapperInput(shard.getFileShard());

                // Blocking RPC Call
                MapperOutput output = workerInfo.getClient().callMapper(input);

                shard.setStatus(TaskStatus.COMPLETED);

                for(int i = 0; i < spec.numPartitions(); i++) {
                    partitionInfos.get(i).addFileName(output.getFiles(i).getFileName());
                }

                workerInfo.setWorkerStatus(WorkerStatus.IDLE);
                latch.countDown();

            } catch(StatusRuntimeException e) {
                shard.setStatus(TaskStatus.READY);
                readyShards.offer(shardId);

                if(handleWorkerFailure(workerInfo, e)) {
                    return;
                }
            }
        }
    }

    private MapperInput buildMapperInput(FileShard fileShard) {
        MapperInput.Builder builder = MapperInput.newBuilder()
                .setAppId(spec.appId())
                .setNumPartitions(spec.numPartitions())
                .setOutputDir(spec.outputDir());

        for(int i = 0; i < fileShard.getFileNames().size(); i++) {
            String fileName = fileShard.getFileNames().get(i);
            FileShard.OffsetRange range = fileShard.getOffsetRanges().get(i);

            builder.addInputSplits(
                    ShardSplit.newBuilder()
                            .setFileName(fileName)
                            .setStartPos(range.startOffset())
                            .setEndPos(range.endOffset())
                            .build()
            );
        }

        return builder.build();
    }

    private boolean handleWorkerFailure(WorkerInfo worker, StatusRuntimeException e) {
        Status.Code code = e.getStatus().getCode();

        if (code == Status.Code.DEADLINE_EXCEEDED) {
            worker.setWorkerStatus(WorkerStatus.SLOW);
            return true;
        }

        if (code == Status.Code.UNAVAILABLE) {
            worker.setWorkerStatus(WorkerStatus.DEAD);
            return true;
        }

        worker.setWorkerStatus(WorkerStatus.IDLE);
        return false;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for tasks", e);
        }
    }
}
