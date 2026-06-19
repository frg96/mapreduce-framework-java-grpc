package com.frg96.mapreduce.client;

import com.frg96.mapreduce.api.MapReduceJob;

/**
 * Command-line entry point for running a MapReduce job.
 *
 * <p>The program accepts the path to a job configuration file, creates
 * an in-process master, and coordinates the configured standalone workers.
 * It exits with status {@code 0} when the job succeeds and status {@code 1}
 * when validation or execution fails.</p>
 */
public final class MapReduceMain {
    private MapReduceMain() {}

    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: MapReduceMain <config.properties>");
            System.exit(1);
        }

        try {
            boolean successful = new MapReduceJob().run(args[0]);
            System.exit(successful ? 0 : 1);
        } catch (Exception e) {
            System.err.println("MapReduce job failed: " + e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
