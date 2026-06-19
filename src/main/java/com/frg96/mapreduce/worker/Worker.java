package com.frg96.mapreduce.worker;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Hosts the MapReduce worker service in a gRPC server.
 *
 * <p>A worker accepts mapper and reducer requests from a master and executes
 * them using {@link MRFrameworkService}. Request handling is delegated to a
 * fixed-size executor based on the number of available processors.</p>
 *
 * <p>The current server uses insecure credentials and binds to the port
 * extracted from the configured {@code host:port} address.</p>
 */
public class Worker {
    private static final Logger LOGGER = Logger.getLogger(Worker.class.getName());

    private final String address;

    private final MRFrameworkService service;

    private ExecutorService executor;

    private Server server;

    /**
     * Creates a worker for the given address.
     *
     * <p>The server is not started until {@link #start()} is called.</p>
     *
     * @param address worker address in {@code host:port} format
     */
    public Worker(String address) {
        this.address = address;
        this.service = new MRFrameworkService(address);
    }

    /**
     * Starts the gRPC server and its request-execution thread pool.
     *
     * @throws IllegalArgumentException if the configured address does not
     *                                  contain a valid port
     * @throws RuntimeException if the gRPC server cannot be started
     */
    public void start() {
        try {
            int port = parsePort(address);

            int threadCount = Runtime.getRuntime().availableProcessors();

            this.executor = Executors.newFixedThreadPool(threadCount);

            this.server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                    .addService(this.service)
                    .executor(executor)
                    .build()
                    .start();

            LOGGER.log(
                    Level.INFO,
                    "Worker started at {0}; listening on port {1} with {2} request threads",
                    new Object[]{address, port, threadCount}
            );

        } catch (IOException e) {
            throw new IllegalStateException("Failed to start worker at " + address, e);
        }

    }

    /**
     * Gracefully stops the gRPC server and request executor.
     *
     * <p>Each component is given up to 30 seconds to terminate before a forced
     * shutdown is requested.</p>
     *
     * @throws InterruptedException if interrupted while waiting for termination
     */
    public void stop() throws InterruptedException {
        LOGGER.log(Level.INFO, "Stopping worker at {0}", address);

        if (this.server != null) {
            this.server.shutdown();
            if(!this.server.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warning("gRPC server did not terminate gracefully; forcing shutdown");
                this.server.shutdownNow();
            }
        }

        if (this.executor != null) {
            this.executor.shutdown();
            if (!this.executor.awaitTermination(30, TimeUnit.SECONDS)) {
                LOGGER.warning("Worker executor did not terminate gracefully; forcing shutdown");
                this.executor.shutdownNow();
            }
        }

        LOGGER.log(Level.INFO, "Worker stopped at {0}", address);
    }

    /**
     * Blocks the calling thread until the gRPC server terminates.
     *
     * <p>This keeps a standalone worker process alive because gRPC uses daemon
     * threads internally. If the server has not been started, this method
     * returns immediately.</p>
     *
     * @throws InterruptedException if the waiting thread is interrupted
     */
    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    /**
     * Extracts the port from a worker address.
     *
     * @param address worker address in {@code host:port} format
     * @return parsed port number
     * @throws IllegalArgumentException if the address has no port separator
     *                                  or the port is not a valid integer
     */
    private static int parsePort(String address) {
        int colon = address.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalArgumentException("Expected host:port, got: " + address);
        }
        return Integer.parseInt(address.substring(colon + 1));
    }
}
