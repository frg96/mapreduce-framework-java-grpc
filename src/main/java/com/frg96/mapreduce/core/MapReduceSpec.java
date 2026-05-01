package com.frg96.mapreduce.core;

import java.util.List;

/**
 * MapReduce specification object based on the config.properties file.
 * @param numWorkers number of workers
 * @param workerAddresses comma-separated list of worker addresses &lt;host:port,[host:port,...]&gt;
 * @param inputFiles comma-separated list of input files paths
 * @param outputDir output directory path
 * @param numPartitions number of Reducer partitions / output files generated
 * @param shardSizeKb size of each mapper input file shard in KB
 * @param appId application ID
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
