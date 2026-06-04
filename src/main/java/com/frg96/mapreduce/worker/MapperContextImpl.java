package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.api.MapperContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Implementation of the {@link MapperContext} interface.
 * Handles partitioning, buffering, sorting, and flushing mapper output.
 */
class MapperContextImpl implements MapperContext {

    private final int numPartitions;

    private final List<Path> intermediateFiles;

    /**
     * A list of TreeMap objects, where each list index corresponds to a partition.
     * The TreeMap objects are used to store sorted key-value pairs after mapping.
     * The keys in the TreeMap objects are the original keys from the input data.
     * The values in the TreeMap objects are lists of values associated with the original key.
     */
    private final List<TreeMap<String, List<String>>> partitionedKeyValues;

    MapperContextImpl(int numPartitions, List<Path> intermediateFiles) {
        this.numPartitions = numPartitions;
        this.intermediateFiles = intermediateFiles;
        this.partitionedKeyValues = new ArrayList<>(numPartitions);

        for(int i = 0; i < numPartitions; i++) {
            partitionedKeyValues.add(new TreeMap<>());
        }

    }

    /**
     * Emits a key-value pair after mapping.
     * Stores the key-value pair in the appropriate in-memory partition based on the key's hash.
     * @param key   key to emit after mapping
     * @param value value to emit after mapping
     */
    @Override
    public void emit(String key, String value) {
        int partitionId = Math.floorMod(key.hashCode(), numPartitions);

        partitionedKeyValues
                .get(partitionId)
                .computeIfAbsent(key, (ignored) -> new ArrayList<>())
                .add(value);
    }

    /**
     * Writes in-memory partitioned key-value pairs to the correct intermediate files.
     * @throws IOException if an I/O error occurs
     */
    void flush() throws IOException {
        for(int i = 0; i < numPartitions; i++) {
            Path file = intermediateFiles.get(i);
            // Files.createDirectories(file.getParent());

            try(BufferedWriter writer = Files.newBufferedWriter(
                    file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            )) {
                for(Map.Entry<String, List<String>> entry : partitionedKeyValues.get(i).entrySet()) {
                    for(String value : entry.getValue()) {
                        writer.write(entry.getKey());
                        writer.write(" ");
                        writer.write(value);
                        writer.newLine();
                    }
                }
            }
        }
    }
}
