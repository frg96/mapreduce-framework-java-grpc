package com.frg96.mapreduce.master;

final class WorkerInfo {
    private final int id;
    private final String address;
    private final MRFrameworkClient client;

    // this must be volatile
    private volatile WorkerStatus workerStatus = WorkerStatus.IDLE;

    WorkerInfo(int id, String address) {
        this.id = id;
        this.address = address;
        this.client = new MRFrameworkClient(address);
    }

    int getId() {
        return this.id;
    }

    String getAddress() {
        return this.address;
    }

    MRFrameworkClient getClient() {
        return this.client;
    }

    WorkerStatus getWorkerStatus() {
        return this.workerStatus;
    }

    void setWorkerStatus(WorkerStatus status) {
        this.workerStatus = status;
    }

    void shutdownClient() {
        this.client.shutdown();
    }

}
