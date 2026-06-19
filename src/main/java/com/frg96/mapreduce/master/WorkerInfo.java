package com.frg96.mapreduce.master;

/**
 * Stores the master's state and gRPC client for one worker.
 *
 * <p>Each instance owns a client connected to the worker's address. Worker
 * status is volatile so updates made by task-execution threads are visible
 * across the master.</p>
 */
final class WorkerInfo {
    private final int id;
    private final String address;
    private final MRFrameworkClient client;

    // This must be volatile as it is accessed by multiple task threads.
    // Volatile guarantees that status updates are immediately visible across threads;
    // each update is a single assignment, so no compound operation or invariant requires synchronized locking.
    private volatile WorkerStatus workerStatus = WorkerStatus.IDLE;

    /**
     * Creates an idle worker record and its gRPC client.
     *
     * @param id zero-based worker identifier
     * @param address worker address in {@code host:port} format
     */
    WorkerInfo(int id, String address) {
        this.id = id;
        this.address = address;
        this.client = new MRFrameworkClient(address);
    }

    /**
     * Returns the worker identifier.
     *
     * @return zero-based worker identifier
     */
    int getId() {
        return this.id;
    }

    /**
     * Returns the worker's network address.
     *
     * @return worker address in {@code host:port} format
     */
    String getAddress() {
        return this.address;
    }

    /**
     * Returns the client used to execute tasks on this worker.
     *
     * @return worker's gRPC client
     */
    MRFrameworkClient getClient() {
        return this.client;
    }

    /**
     * Returns the worker's current availability status.
     *
     * @return current worker status
     */
    WorkerStatus getWorkerStatus() {
        return this.workerStatus;
    }

    /**
     * Updates the worker's availability status.
     *
     * @param status new worker status
     */
    void setWorkerStatus(WorkerStatus status) {
        this.workerStatus = status;
    }

    /**
     * Shuts down the gRPC client and releases its underlying channel.
     */
    void shutdownClient() {
        this.client.shutdown();
    }

}
