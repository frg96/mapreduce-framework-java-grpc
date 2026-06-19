package com.frg96.mapreduce.core;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages mapper and reducer intermediate-output directories.
 */
public class IntermediateDirectoryHandler {
    private static final Logger LOGGER = Logger.getLogger(IntermediateDirectoryHandler.class.getName());

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
                                    throw new UncheckedIOException(e);
                                }
                            });
                }
            }

            Files.createDirectories(Path.of(intermediateDir.toString(), "mapper"));
            Files.createDirectories(Path.of(intermediateDir.toString(), "reducer"));

            LOGGER.log(
                    Level.INFO,
                    "Prepared intermediate directory: {0}",
                    intermediateDir
            );

            return true;

        } catch (IOException | UncheckedIOException e ) {
            LOGGER.log(
                    Level.SEVERE,
                    "Failed to prepare intermediate directory: " + intermediateDir,
                    e
            );
            return false;
        }
    }
}
