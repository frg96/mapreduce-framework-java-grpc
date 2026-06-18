package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.apps.BuiltInApps;

public class WorkerMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("Usage: WorkerMain <host:port>");
            System.exit(1);
        }

        BuiltInApps.registerAll();

        String address = args[0];

        System.out.println("Starting server on " + address);
        final Worker worker = new Worker(address);
        worker.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Use stderr here since the logger may have been reset by its JVM shutdown hook.
            System.err.println("*** shutting down gRPC server since JVM is shutting down");
            try {
                worker.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        worker.blockUntilShutdown();
    }
}
