package com.frg96.mapreduce.api;

import com.frg96.mapreduce.core.*;
import com.frg96.mapreduce.master.Master;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Synchronous entry point for executing a MapReduce job.
 *
 * <p>A job is parsed and validated, its intermediate directory is prepared,
 * input files are divided into shards, and an in-process master coordinates
 * the configured workers through gRPC.</p>
 */
public final class MapReduceJob {
    private static final Logger LOGGER = Logger.getLogger(MapReduceJob.class.getName());

    /**
     * Executes the job described by a configuration file.
     *
     * <p><ol>
     * <li>Parses the config file</li>
     * <li>Validates the spec</li>
     * <li>Prepares the intermediate directory</li>
     * <li>Shards the input files</li>
     * <li>Starts the master</li>
     * <li>Waits for the master to finish</li>
     * </ol></p>
     *
     * @param configFilePath path to the job config properties file
     * @return {@code true} when the job completes successfully; otherwise {@code false}
     */
    public boolean run(String configFilePath) {
        LOGGER.log(Level.INFO, "Loading job configuration from {0}", configFilePath);

        // Parse the config file and validate the spec
        MapReduceSpec spec = SpecParser.parse(configFilePath);

        if(!SpecValidator.validate(spec)) {
            return false;
        }

        LOGGER.log(
                Level.INFO,
                "Starting application {0} with {1} workers and {2} partitions",
                new Object[]{spec.appId(), spec.numWorkers(), spec.numPartitions()}
        );

        // Prepare the intermediate directory
        if(!IntermediateDirectoryHandler.prepareIntermediateDirectory(spec.outputDir())) {
            return false;
        }

        // Shard the input files
         List<FileShard> fileShards = FileSharder.shardFiles(spec);

        // Run master
        Master master = new Master(spec, fileShards);
        boolean successful = master.run();
        LOGGER.log(
                successful ? Level.INFO : Level.SEVERE,
                successful ? "Job completed successfully" : "Job execution failed"
        );

        return successful;

    }
}
