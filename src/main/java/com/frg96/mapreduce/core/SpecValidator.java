package com.frg96.mapreduce.core;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for validating the spec.
 */
public class SpecValidator {
    private SpecValidator() {}

    /**
     * Validates the MapReduceSpec object.
     * @param spec the MapReduceSpec object to validate
     * @return true if the spec is valid, false otherwise
     */
    public static boolean validate(MapReduceSpec spec) {
        if(spec.numWorkers() <= 0) {
            System.err.println("number of workers should be greater than 0");
            return false;
        }

        if(spec.numWorkers() != spec.workerAddresses().size()) {
            System.err.println("number of workers should be equal to the number of worker addresses");
            return false;
        }

        for(String workerAddress: spec.workerAddresses()) {
            if(workerAddress == null || workerAddress.isBlank()) {
                System.err.println("worker address cannot be null or empty");
                return false;
            }
            if(!workerAddress.matches("^([a-zA-Z0-9.-]+):([1-9][0-9]{0,4})$")) {
                System.err.println("worker address: " + workerAddress + " must be in the format <host>:<port>");
                return false;
            }
        }

        if(spec.inputFiles().isEmpty()) {
            System.err.println("at least one input file must be specified");
            return false;
        }

        for(InputFile inputFile: spec.inputFiles()) {
            Path path = Path.of(inputFile.filePath());
            if(Files.notExists(path)){
                System.err.println("input file does not exist: " + path);
                return false;
            }
            if (!Files.isRegularFile(path)) {
                System.err.println("input file is not regular file: " + path);
                return false;
            }
            if (inputFile.fileSizeBytes() <= 0) {
                System.err.println("input file is empty: " + path);
                return false;
            }
        }

        if(spec.outputDir() == null || spec.outputDir().isBlank()) {
            System.err.println("output directory cannot be null or empty");
            return false;
        }

        Path outputDirPath = Path.of(spec.outputDir());

        if(!Files.isDirectory(outputDirPath)) {
            System.err.println("output directory does not exist: " + outputDirPath);
            return false;
        }

        if(spec.numPartitions() <= 0) {
            System.err.println("number of partitions should be greater than 0");
            return false;
        }

        if(spec.shardSizeKb() <= 0) {
            System.err.println("shard size should be greater than 0");
            return false;
        }

        if(spec.appId() == null || spec.appId().isBlank()) {
            System.err.println("app id cannot be null or empty");
            return false;
        }



        return true;
    }
}
