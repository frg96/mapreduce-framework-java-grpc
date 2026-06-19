package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.apps.BuiltInApps;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Command-line entry point for running a standalone MapReduce worker.
 *
 * <p>The program registers all built-in applications, starts a gRPC worker
 * at the supplied address, installs a JVM shutdown hook for graceful
 * cleanup, and blocks until the server terminates.</p>
 */
public class WorkerMain {
    private static final Logger LOGGER = Logger.getLogger(WorkerMain.class.getName());

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
            LOGGER.severe("Usage: WorkerMain <host:port>");
            System.exit(1);
        }

        BuiltInApps.registerAll();

        String address = args[0];

        final Worker worker = new Worker(address);
        worker.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Using stderr too, since the logger may have been reset by its JVM shutdown hook.
            System.err.println("JVM shutdown requested for worker " + address);
            if(LOGGER != null)
                LOGGER.log(Level.INFO, "JVM shutdown requested for worker {0}", address);

            try {
                worker.stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.log(Level.WARNING, "Worker shutdown was interrupted", e);
            }
        }, "worker-shutdown"));

        worker.blockUntilShutdown();
    }
}
