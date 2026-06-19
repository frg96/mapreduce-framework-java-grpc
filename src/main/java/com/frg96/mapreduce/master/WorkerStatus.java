package com.frg96.mapreduce.master;

/**
 * Availability state of a worker known to the master.
 */
enum WorkerStatus {
    /** The worker is available to execute a task. */
    IDLE,

    /** The worker is currently executing a task. */
    BUSY,

    /** The worker exceeded an RPC deadline and is no longer assigned tasks. */
    SLOW,

    /** The worker is unavailable and is no longer assigned tasks. */
    DEAD
}
