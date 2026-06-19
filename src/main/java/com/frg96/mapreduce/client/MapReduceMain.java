package com.frg96.mapreduce.client;

import com.frg96.mapreduce.api.MapReduceJob;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Command-line entry point for running a MapReduce job.
 *
 * <p>The program accepts the path to a job configuration file, creates
 * an in-process master, and coordinates the configured standalone workers.
 * It exits with status {@code 0} when the job succeeds and status {@code 1}
 * when validation or execution fails.</p>
 */
public final class MapReduceMain {
    private static final Logger LOGGER = Logger.getLogger(MapReduceMain.class.getName());

    private MapReduceMain() {}

    public static void main(String[] args) {
        if (args.length != 1) {
            LOGGER.severe("Usage: MapReduceMain <config.properties>");
            System.exit(1);
        }

        try {
            boolean successful = new MapReduceJob().run(args[0]);

            if (successful) {
                LOGGER.info("MapReduce job completed successfully");
            } else {
                LOGGER.severe("MapReduce job failed");
            }

            System.exit(successful ? 0 : 1);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "MapReduce job terminated unexpectedly", e);
            System.exit(1);
        }
    }
}
