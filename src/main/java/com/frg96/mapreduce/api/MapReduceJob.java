package com.frg96.mapreduce.api;

import com.frg96.mapreduce.core.*;

import java.util.List;

/**
 * Main class for configuring and running a MapReduce job.
 */
public final class MapReduceJob {
    /**
     * Runs a MapReduce job.
     * 1. Parses the config file
     * 2. Validates the spec
     * 3. Prepares the intermediate directory
     * 4. Shards the input files
     * 5. Starts the master
     * 6. Waits for the master to finish
     *
     * @param configFilePath path to the config.properties file
     * @return true if the job was successful, false otherwise
     */
    public boolean run(String configFilePath) {
        // Parse the config file and validate the spec
        MapReduceSpec spec = SpecParser.parse(configFilePath);

        if(!SpecValidator.validate(spec)) {
            return false;
        }

        // Prepare the intermediate directory
        if(!IntermediateDirectoryHandler.prepareIntermediateDirectory(spec.outputDir())) {
            return false;
        }

        // Shard the input files
         List<FileShard> fileShards = FileSharder.shardFiles(spec);

        // Start master
        // TODO
//        Master master = new Master(spec, fileShards);
//        return master.run();

        return true;
    }
}
