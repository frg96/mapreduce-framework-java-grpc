package com.frg96.mapreduce.core;

import java.util.List;

/**
 * Immutable configuration for one MapReduce job.
 *
 * @param numWorkers number of workers expected by the master
 * @param workerAddresses comma-separated list of worker addresses &lt;host:port,[host:port,...]&gt;
 * @param inputFiles comma-separated list of input files paths processed by mapper tasks
 * @param outputDir existing output directory path for intermediate and final output
 * @param numPartitions number of reducer partitions and the number of final output files generated
 * @param shardSizeKb target size of each mapper input file shard in kilobytes
 * @param appId application ID resolved through the worker task registry
 */
public record MapReduceSpec(
        int numWorkers,
        List<String> workerAddresses,
        List<InputFile> inputFiles,
        String outputDir,
        int numPartitions,
        int shardSizeKb,
        String appId
) {}
