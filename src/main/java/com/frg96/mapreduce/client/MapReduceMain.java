package com.frg96.mapreduce.client;

import com.frg96.mapreduce.api.MapReduceJob;

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
