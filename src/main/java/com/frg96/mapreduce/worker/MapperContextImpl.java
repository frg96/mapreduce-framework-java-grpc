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
 * Worker-side implementation of {@link MapperContext}.
 *
 * <p>Emitted key-value pairs are partitioned using the key's hash. Each
 * partition stores its keys in lexicographical order so its intermediate
 * file can later participate in the reducer's k-way merge.</p>
 *
 * <p>This context is created for one mapper task and is not thread-safe.</p>
 */
class MapperContextImpl implements MapperContext {

    private final int numPartitions;

    private final List<Path> intermediateFiles;

    /**
     * In-memory mapper output indexed by partition ID.
     *
     * <p>This is a list of {@link TreeMap} objects, where each list index corresponds to a partition.
     * The TreeMap objects are used to store sorted key-value pairs after mapping.
     * The keys in the TreeMap objects are the original keys from the input data.
     * The values in the TreeMap objects are lists of values associated with the original key.</p>
     */
    private final List<TreeMap<String, List<String>>> partitionedKeyValues;

    /**
     * Creates a mapper context for the given output partitions.
     *
     * @param numPartitions number of reducer partitions
     * @param intermediateFiles output file for each partition; its size and
     *                          ordering must correspond to
     *                          {@code numPartitions}
     */
    MapperContextImpl(int numPartitions, List<Path> intermediateFiles) {
        this.numPartitions = numPartitions;
        this.intermediateFiles = intermediateFiles;
        this.partitionedKeyValues = new ArrayList<>(numPartitions);

        for(int i = 0; i < numPartitions; i++) {
            partitionedKeyValues.add(new TreeMap<>());
        }

    }

    /**
     * Buffers a mapper result in the partition selected by the key's hash.
     *
     * <p>{@link Math#floorMod(int, int)} ensures that negative hash codes still
     * produce a valid partition index.</p>
     *
     * @param key key emitted by the mapper
     * @param value value associated with the key
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
     * Appends all buffered mapper results to their partition-specific
     * intermediate files.
     *
     * <p>Files are created when absent. Records are written in ascending key
     * order, one key-value pair per line, with a space separating the fields.
     * Buffered entries are not cleared by this method, so a mapper context
     * should normally be flushed only once.</p>
     *
     * @throws IOException if an intermediate file cannot be opened or written
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
