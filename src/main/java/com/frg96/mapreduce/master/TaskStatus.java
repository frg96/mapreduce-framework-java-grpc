package com.frg96.mapreduce.master;

/**
 * Execution state of a map or reduce task.
 */
enum TaskStatus {
    /** The task is available to be assigned to a worker. */
    READY,

    /** The task is currently being executed by a worker. */
    IN_PROGRESS,

    /** The task completed successfully. */
    COMPLETED
}
