package com.frg96.mapreduce.core;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Manages mapper and reducer intermediate-output directories.
 */
public class IntermediateDirectoryHandler {
    private IntermediateDirectoryHandler() {}

    /**
     * Recreates {@code outputDir/intermediate} for a new job.
     *
     * <p>Existing intermediate contents are deleted before empty
     * {@code mapper} and {@code reducer} directories are created.</p>
     *
     * @param outputDir existing job output directory
     * @return {@code true} when preparation succeeds; otherwise {@code false}
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
