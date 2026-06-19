package com.frg96.mapreduce.master;

import com.frg96.mapreduce.*;
import com.frg96.mapreduce.core.FileShard;
import com.frg96.mapreduce.core.MapReduceSpec;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

/**
 * Coordinates the execution of a single MapReduce job.
 *
 * <p>The master assigns input shards to workers during the map phase,
 * collects the generated intermediate files, and assigns output partitions
 * during the reduce phase. Workers are contacted through blocking gRPC
 * clients.</p>
 *
 * <p>Failed tasks are returned to their ready queue for reassignment.
 * Workers that become unavailable or exceed their deadline are excluded
 * from subsequent work. The job is aborted when no usable workers remain.</p>
 *
 * <p>A {@code Master} instance represents one job and should not be reused.</p>
 */
public class Master {
    private static final Logger logger = Logger.getLogger(Master.class.getName());

    private final MapReduceSpec spec;
    private final List<WorkerInfo> workerInfos = new ArrayList<>();
    private final List<ShardInfo> shardInfos = new ArrayList<>();
    private final List<PartitionInfo> partitionInfos = new ArrayList<>();

    // Task Queues to hold the IDs of ShardInfo or PartitionInfo
    private final BlockingQueue<Integer> readyShards = new LinkedBlockingQueue<>();
    private final BlockingQueue<Integer> readyPartitions = new LinkedBlockingQueue<>();

    private static final int POISON_PILL = -1; // to signal mapper and reducer threads to stop

    /**
     * Creates a master for a MapReduce job.
     *
     * @param spec validated job configuration
     * @param fileShards input shards to process during the map phase
     */
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

    /**
     * Executes the map and reduce phases and releases worker clients when done.
     *
     * @return {@code true} if both phases complete successfully;
     *         {@code false} if preparation or task execution fails
     */
    public boolean run() {
        try{
            if(!prepareFinalOutputDirectory(spec.outputDir()))
                return false;

            if(!runMapPhase())
                return false;

            if(!runReducePhase())
                return false;

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

    /**
     * Executes the map phase by assigning ready input shards to workers.
     *
     * @return {@code true} if every shard completes successfully;
     *         {@code false} if the phase is aborted
     */
    private boolean runMapPhase() {
        CountDownLatch latch = new CountDownLatch(shardInfos.size());
        AtomicBoolean aborted = new AtomicBoolean(false);

        for (ShardInfo shardInfo : shardInfos) {
            readyShards.offer(shardInfo.getId());
        }

        try (ExecutorService executor = Executors.newFixedThreadPool(workerInfos.size())) {
            for (WorkerInfo workerInfo : workerInfos) {
                executor.submit(() -> mapperTask(workerInfo, latch, aborted));
            }

            await(latch);

            for (int i = 0; i < workerInfos.size(); i++) {
                readyShards.offer(POISON_PILL);
            }
        } // executor will auto close

        return !aborted.get();
    }

    /**
     * Repeatedly takes ready shards from the work queue and executes them
     * on the given worker. Failed shards are returned to the queue so another
     * usable worker can retry them.
     *
     * @param workerInfo worker assigned to consume map tasks
     * @param latch tracks the number of incomplete shards
     * @param aborted shared flag indicating that the phase must stop
     */
    private void mapperTask(WorkerInfo workerInfo, CountDownLatch latch, AtomicBoolean aborted) {
        while(!Thread.currentThread().isInterrupted() && !aborted.get()) {
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

            if(aborted.get()) {
                return;
            }

            ShardInfo shard = shardInfos.get(shardId);
            shard.setStatus(TaskStatus.IN_PROGRESS);
            workerInfo.setWorkerStatus(WorkerStatus.BUSY);

            try {
                MapperInput input = buildMapperInput(shard.getFileShard());

                // Blocking RPC Call
                MapperOutput output = workerInfo.getClient().callMapper(input);

                if(aborted.get()) {
                    return;
                }

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
                    abortIfNoUsableWorkers(latch, aborted);
                    return;
                }
            }
        }
    }

    /**
     * Creates the mapper RPC request for an input shard.
     *
     * <p>The request includes the application ID, output partition count,
     * output directory, and byte ranges assigned to the shard.</p>
     *
     * @param fileShard input files and byte ranges assigned to the task
     * @return mapper request to send to a worker
     */
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

    /**
     * Executes the reduce phase using workers that successfully completed
     * the map phase and remain available.
     *
     * @return {@code true} if every partition completes successfully;
     *         {@code false} if no active workers remain or the phase is aborted
     */
    private boolean runReducePhase() {
        List<WorkerInfo> activeWorkers = workerInfos.stream()
                .filter(workerInfo -> workerInfo.getWorkerStatus() != WorkerStatus.DEAD)
                .filter(workerInfo -> workerInfo.getWorkerStatus() != WorkerStatus.SLOW)
                .toList();

        if(activeWorkers.isEmpty()) {
            logger.severe("No active workers available for reduce phase");
            return false;
        }

        CountDownLatch latch = new CountDownLatch(partitionInfos.size());
        AtomicBoolean aborted = new AtomicBoolean(false);

        for(PartitionInfo partitionInfo: partitionInfos) {
            readyPartitions.offer(partitionInfo.getId());
        }

        try(ExecutorService executor = Executors.newFixedThreadPool(activeWorkers.size())) {
            for(WorkerInfo workerInfo: activeWorkers) {
                executor.submit(() -> reducerTask(workerInfo, latch, aborted));
            }

            await(latch);

            for(int i = 0; i < activeWorkers.size(); i++) {
                readyPartitions.offer(POISON_PILL);
            }
        } // executor will auto close

        return !aborted.get();
    }

    /**
     * Repeatedly takes ready partitions from the work queue and executes them
     * on the given worker. Failed partitions are returned to the queue so
     * another usable worker can retry them.
     *
     * @param workerInfo worker assigned to consume reduce tasks
     * @param latch tracks the number of incomplete partitions
     * @param aborted shared flag indicating that the phase must stop
     */
    private void reducerTask(WorkerInfo workerInfo, CountDownLatch latch, AtomicBoolean aborted) {
        while(!Thread.currentThread().isInterrupted() && !aborted.get()) {
            int partitionId;

            try {
                partitionId = readyPartitions.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            if(partitionId == POISON_PILL) {
                return;
            }

            if(aborted.get()) {
                return;
            }

            PartitionInfo partition = partitionInfos.get(partitionId);
            partition.setStatus(TaskStatus.IN_PROGRESS);
            workerInfo.setWorkerStatus(WorkerStatus.BUSY);

            try {
                ReducerInput input = buildReducerInput(partition);

                // Blocking RPC Call
                ReducerOutput output = workerInfo.getClient().callReducer(input);

                if(aborted.get()) {
                    return;
                }

                // copying file to final output dir
                copyReducerOutput(output.getFile().getFileName());

                partition.setStatus(TaskStatus.COMPLETED);
                workerInfo.setWorkerStatus(WorkerStatus.IDLE);
                latch.countDown();

            } catch (StatusRuntimeException e) {
                partition.setStatus(TaskStatus.READY);
                readyPartitions.offer(partitionId);

                if(handleWorkerFailure(workerInfo, e)) {
                    abortIfNoUsableWorkers(latch, aborted);
                    return;
                }
            }
        }
    }

    /**
     * Creates the reducer RPC request for an output partition.
     *
     * <p>The request contains the application ID, partition ID, output
     * directory, and intermediate files produced for the partition.</p>
     *
     * @param partitionInfo partition and its intermediate input files
     * @return reducer request to send to a worker
     */
    private ReducerInput buildReducerInput(PartitionInfo partitionInfo) {
        ReducerInput.Builder builder = ReducerInput.newBuilder()
                .setAppId(spec.appId())
                .setPartitionId(partitionInfo.getId())
                .setOutputDir(spec.outputDir());

        for(String fileName: partitionInfo.getFileNamesSnapshot()) {
            builder.addFiles(
                    File.newBuilder()
                            .setFileName(fileName)
                            .build()
            );
        }

        return builder.build();
    }

    /**
     * Copies a reducer result into the job's final output directory.
     *
     * @param reducerOutputFilePath path to the reducer's intermediate result
     * @throws IllegalStateException if the result cannot be copied
     */
    private void copyReducerOutput(String reducerOutputFilePath) {
        try {
            Path source = Path.of(reducerOutputFilePath);
            Path destination = Path.of(
                    spec.outputDir(),
                    "final",
                    source.getFileName().toString()
            );

            Files.copy(source, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to copy reducer output", e);
        }

    }

    /**
     * Updates a worker's status based on a failed gRPC request.
     *
     * <p>Deadline failures mark the worker as slow, unavailable failures
     * mark it as dead, and other failures return it to the idle state.</p>
     *
     * @param worker worker whose request failed
     * @param e gRPC failure returned by the worker
     * @return {@code true} if the worker should stop consuming tasks;
     *         {@code false} if it remains usable
     */
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

    /**
     * Aborts the current phase when no usable workers remain.
     *
     * <p>The latch is released so the coordinating thread does not wait
     * indefinitely for tasks that can no longer be executed.</p>
     *
     * @param latch tracks tasks that have not completed
     * @param aborted shared flag indicating whether the phase was aborted
     */
    private void abortIfNoUsableWorkers(CountDownLatch latch, AtomicBoolean aborted) {
        if (noUsableWorkers() && aborted.compareAndSet(false, true)) {
            logger.severe("No usable workers remain; aborting job");

            while(latch.getCount() > 0) {
                latch.countDown();
            }
        }
    }

    /**
     * Determines whether every configured worker has been marked dead or slow.
     *
     * @return {@code true} if no worker can accept additional tasks
     */
    private boolean noUsableWorkers() {
        return workerInfos.stream()
                .noneMatch(workerInfo -> (workerInfo.getWorkerStatus() != WorkerStatus.DEAD) && (workerInfo.getWorkerStatus() != WorkerStatus.SLOW));
    }

    /**
     * Waits until every task tracked by the latch has completed or the phase
     * has been aborted.
     *
     * @param latch task-completion latch
     * @throws IllegalStateException if the waiting thread is interrupted
     */
    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for tasks", e);
        }
    }
}
