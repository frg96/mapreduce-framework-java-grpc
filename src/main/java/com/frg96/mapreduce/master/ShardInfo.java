package com.frg96.mapreduce.master;

import com.frg96.mapreduce.core.FileShard;

/**
 * Tracks an input shard and the state of its corresponding map task.
 *
 * <p>The shard identifier and file data are immutable. Status access is
 * synchronized because map tasks may be scheduled and updated by different
 * threads.</p>
 */
final class ShardInfo {
    private final int id;
    private final FileShard fileShard;
    private TaskStatus status = TaskStatus.READY; // Use synchronization

    /**
     * Creates shard tracking information in the
     * {@link TaskStatus#READY} state.
     *
     * @param id zero-based shard identifier
     * @param fileShard input files and byte ranges assigned to the shard
     */
    ShardInfo(int id, FileShard fileShard) {
        this.id = id;
        this.fileShard = fileShard;
    }

    /**
     * Returns the shard identifier.
     *
     * @return zero-based shard identifier
     */
    int getId() {
        return this.id;
    }

    /**
     * Returns the input file ranges represented by this shard.
     *
     * @return associated file shard
     */
    FileShard getFileShard() {
        return this.fileShard;
    }

    /**
     * Returns the current map-task status.
     *
     * @return current task status
     */
    synchronized TaskStatus getStatus() {
        return this.status;
    }

    /**
     * Updates the map-task status.
     *
     * @param status new task status
     */
    synchronized void setStatus(TaskStatus status) {
        this.status = status;
    }
}
