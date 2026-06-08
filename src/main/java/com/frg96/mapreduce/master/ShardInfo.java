package com.frg96.mapreduce.master;

import com.frg96.mapreduce.core.FileShard;

final class ShardInfo {
    private final int id;
    private final FileShard fileShard;
    private TaskStatus status = TaskStatus.READY; // Use synchronization

    ShardInfo(int id, FileShard fileShard) {
        this.id = id;
        this.fileShard = fileShard;
    }

    int getId() {
        return this.id;
    }

    FileShard getFileShard() {
        return this.fileShard;
    }

    synchronized TaskStatus getStatus() {
        return this.status;
    }

    synchronized void setStatus(TaskStatus status) {
        this.status = status;
    }
}
