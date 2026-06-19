package com.frg96.mapreduce.core;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;

/**
 * Validates whether a parsed {@link MapReduceSpec} can be executed.
 *
 * <p>Validation checks worker counts and addresses, input files, the output
 * directory, partition and shard sizes, and the application ID.</p>
 */
public class SpecValidator {
    private static final Logger LOGGER = Logger.getLogger(SpecValidator.class.getName());

    private SpecValidator() {}

    /**
     * Validates a job specification.
     *
     * @param spec parsed job specification
     * @return {@code true} when every validation rule passes, {@code false} otherwise
     */
    public static boolean validate(MapReduceSpec spec) {
        if(spec.numWorkers() <= 0) {
            LOGGER.warning("number of workers should be greater than 0");
            return false;
        }

        if(spec.numWorkers() != spec.workerAddresses().size()) {
            LOGGER.warning("number of workers should be equal to the number of worker addresses");
            return false;
        }

        for(String workerAddress: spec.workerAddresses()) {
            if(workerAddress == null || workerAddress.isBlank()) {
                LOGGER.warning("worker address cannot be null or empty");
                return false;
            }
            if(!workerAddress.matches("^([a-zA-Z0-9.-]+):([1-9][0-9]{0,4})$")) {
                LOGGER.warning("worker address: " + workerAddress + " must be in the format <host>:<port>");
                return false;
            }
        }

        if(spec.inputFiles().isEmpty()) {
            LOGGER.warning("at least one input file must be specified");
            return false;
        }

        for(InputFile inputFile: spec.inputFiles()) {
            Path path = Path.of(inputFile.filePath());
            if(Files.notExists(path)){
                LOGGER.warning("input file does not exist: " + path);
                return false;
            }
            if (!Files.isRegularFile(path)) {
                LOGGER.warning("input file is not regular file: " + path);
                return false;
            }
            if (inputFile.fileSizeBytes() <= 0) {
                LOGGER.warning("input file is empty: " + path);
                return false;
            }
        }

        if(spec.outputDir() == null || spec.outputDir().isBlank()) {
            LOGGER.warning("output directory cannot be null or empty");
            return false;
        }

        Path outputDirPath = Path.of(spec.outputDir());

        if(!Files.isDirectory(outputDirPath)) {
            LOGGER.warning("output directory does not exist: " + outputDirPath);
            return false;
        }

        if(spec.numPartitions() <= 0) {
            LOGGER.warning("number of partitions should be greater than 0");
            return false;
        }

        if(spec.shardSizeKb() <= 0) {
            LOGGER.warning("shard size should be greater than 0");
            return false;
        }

        if(spec.appId() == null || spec.appId().isBlank()) {
            LOGGER.warning("app id cannot be null or empty");
            return false;
        }

        return true;
    }
}
