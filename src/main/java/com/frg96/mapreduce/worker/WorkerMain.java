package com.frg96.mapreduce.worker;

public class WorkerMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: WorkerMain <host:port>");
            System.exit(1);
        }

        String address = args[0];

        System.out.println("Starting server on " + address);
        final Worker worker = new Worker(address);
        worker.start();
        worker.blockUntilShutdown();
    }
}
