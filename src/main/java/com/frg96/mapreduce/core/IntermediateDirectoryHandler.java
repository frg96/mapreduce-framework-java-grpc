package com.frg96.mapreduce.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Utility class for handling the intermediate directory.
 */
public class IntermediateDirectoryHandler {
    private IntermediateDirectoryHandler() {}

    /**
     * Prepares the intermediate directory by deleting all files in it and creating the mapper and reducer directories.
     * @param outputDir the output directory path
     * @return true if the intermediate directory was prepared successfully, false otherwise
     */
    public static boolean prepareIntermediateDirectory(String outputDir) {
        Path intermediateDir = Path.of(outputDir, "intermediate");

        try {
            if(Files.exists(intermediateDir)) {
                try(var paths = Files.walk(intermediateDir)) {
                    paths.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try{
                                    Files.delete(path);
                                } catch (IOException e) {
                                    System.err.println("Failed to delete intermediate file: " + path);
                                    throw new RuntimeException(e);
                                }
                            });
                }
            }

            Files.createDirectories(Path.of(intermediateDir.toString(), "mapper"));
            Files.createDirectories(Path.of(intermediateDir.toString(), "reducer"));

            return true;

        } catch (IOException e) {
            System.err.println("Failed to prepare intermediate directory: " + intermediateDir);
            return false;
        }
    }
}
