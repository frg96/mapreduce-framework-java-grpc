package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.apps.BuiltInApps;

/**
 * Command-line entry point for running a standalone MapReduce worker.
 *
 * <p>The program registers all built-in applications, starts a gRPC worker
 * at the supplied address, installs a JVM shutdown hook for graceful
 * cleanup, and blocks until the server terminates.</p>
 */
public class WorkerMain {
    /**
     * Starts a standalone worker process.
     *
     * @param args command-line arguments containing exactly one worker address
     *             in {@code host:port} format
     * @throws Exception if the worker cannot start or is interrupted while
     *                   waiting for shutdown
     */
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
