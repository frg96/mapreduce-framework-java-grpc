package com.frg96.mapreduce.worker;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Worker class that runs the MRFrameworkService and handles gRPC server startup and shutdown.
 */
public class Worker {
    private static final Logger logger = Logger.getLogger(Worker.class.getName());

    private final String address;

    private final MRFrameworkService service;

    private Server server;

    public Worker(String address) {
        this.address = address;
        this.service = new MRFrameworkService(address);
    }

    public void start() {
        try {
            int port = parsePort(address);

            final ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

            this.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                    .addService(this.service)
                    .executor(executor)
                    .build()
                    .start();

            logger.info("Server started, listening on " + port);

            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                    System.err.println("*** shutting down gRPC server since JVM is shutting down");
                    try {
                        Worker.this.stop();
                    } catch (InterruptedException e) {
                        if (server != null) {
                            server.shutdownNow();
                        }
                        e.printStackTrace(System.err);
                    } finally {
                        executor.shutdown();
                    }
                    System.err.println("*** server shut down");
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void stop() throws InterruptedException {
        if (this.server != null) {
            this.server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static int parsePort(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Expected host:port, got: " + address);
        }
        return Integer.parseInt(address.substring(colon + 1));
    }
}
