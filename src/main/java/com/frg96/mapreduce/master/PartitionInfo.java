package com.frg96.mapreduce.master;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks the state and intermediate files associated with one reduce
 * partition.
 *
 * <p>Mapper tasks may add files concurrently, so file and status access is
 * synchronized. File names are exposed through immutable snapshots.</p>
 */
final class PartitionInfo {
    private final int id;
    private final List<String> fileNames = new ArrayList<>();
    private TaskStatus status = TaskStatus.READY; // Use synchronization

    /**
     * Creates an empty partition in the {@link TaskStatus#READY} state.
     *
     * @param id zero-based partition identifier
     */
    PartitionInfo(int id) {
        this.id = id;
    }

    /**
     * Returns the partition identifier.
     *
     * @return zero-based partition identifier
     */
    int getId() {
        return this.id;
    }

    /**
     * Adds a mapper-generated intermediate file to this partition.
     *
     * @param fileName path to the intermediate file
     */
    synchronized void addFileName(String fileName) {
        fileNames.add(fileName);
    }

    /**
     * Returns an immutable snapshot of the intermediate files currently
     * assigned to this partition.
     *
     * @return immutable file-name snapshot
     */
    synchronized List<String> getFileNamesSnapshot() {
        return List.copyOf(fileNames);
    }

    /**
     * Returns the current reduce-task status.
     *
     * @return current task status
     */
    synchronized TaskStatus getStatus() {
        return status;
    }

    /**
     * Updates the reduce-task status.
     *
     * @param status new task status
     */
    synchronized void setStatus(TaskStatus status) {
        this.status = status;
    }
}

