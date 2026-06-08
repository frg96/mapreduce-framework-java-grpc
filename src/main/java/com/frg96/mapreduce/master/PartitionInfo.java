package com.frg96.mapreduce.master;

import java.util.ArrayList;
import java.util.List;

final class PartitionInfo {
    private final int id;
    private final List<String> fileNames = new ArrayList<>();
    private TaskStatus status = TaskStatus.READY; // Use synchronization

    PartitionInfo(int id) {
        this.id = id;
    }

    int getId() {
        return this.id;
    }

    synchronized void addFileName(String fileName) {
        fileNames.add(fileName);
    }

    synchronized List<String> getFileNamesSnapshot() {
        return List.copyOf(fileNames);
    }

    synchronized TaskStatus getStatus() {
        return status;
    }

    synchronized void setStatus(TaskStatus status) {
        this.status = status;
    }
}

