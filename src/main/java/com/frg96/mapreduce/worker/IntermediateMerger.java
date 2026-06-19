package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.File;
import com.frg96.mapreduce.api.Reducer;
import com.frg96.mapreduce.api.ReducerContext;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.logging.Logger;

/**
 * Merges sorted mapper output files and groups their values by key for
 * reduction.
 *
 * <p>Each intermediate file must be sorted by key and contain one
 * whitespace-delimited key-value pair per line. A k-way merge uses a
 * priority queue to process keys in ascending lexicographical order without
 * loading every file into memory.</p>
 */
class IntermediateMerger {
    private static final Logger logger = Logger.getLogger(IntermediateMerger.class.getName());

    /**
     * Entry read from one intermediate file during the k-way merge.
     *
     * @param key intermediate key used for ordering and grouping
     * @param value value associated with the key
     * @param fileIndex index of the reader from which the entry originated
     */
    private static record HeapEntry(String key, String value, int fileIndex) implements Comparable<HeapEntry>{
        /**
         * Compares entries lexicographically by key.
         *
         * @param other entry to compare against
         * @return comparison result for the two keys
         */
        @Override
        public int compareTo(HeapEntry other) {
            return this.key().compareTo(other.key());
        }
    }

    /**
     * Reads the next key-value entry from an intermediate file.
     *
     * @param reader reader positioned at the next intermediate record
     * @param fileIndex index identifying the entry's source reader
     * @return the next heap entry, or {@code null} at end of file
     * @throws IOException if the intermediate file cannot be read
     */
    private static HeapEntry readEntry(BufferedReader reader, int fileIndex) throws IOException {
        String line = reader.readLine();

        if(line == null)
            return null;

        String[] lineSplit = line.split("\\s+");
        String key = lineSplit[0];
        String value = lineSplit[1];

        return new HeapEntry(key, value, fileIndex);
    }

    /**
     * Merges sorted intermediate files and invokes the reducer once for each
     * distinct key.
     *
     * <p>The method keeps one entry from each input file in a minimum heap.
     * Values with equal keys are collected and passed together to
     * {@link Reducer#reduce(String, List, ReducerContext)}. All opened readers
     * are closed before this method returns.</p>
     *
     * @param reducer user-defined reducer that processes grouped values
     * @param filesList sorted intermediate files for one partition
     * @param reducerContext context that receives reducer output
     * @throws IOException if an intermediate file cannot be opened, read, or closed
     */
    static void mergeAndReduce(Reducer reducer, List<File> filesList, ReducerContext reducerContext) throws IOException {
        // Using k-way merge with a min-heap (using PriorityQueue)
        // minHeap always gives us the next smallest key, regardless of which file it’s in
        PriorityQueue<HeapEntry> minHeap = new PriorityQueue<>();

        List<BufferedReader> readers = new ArrayList<>();

        try {
            // Open all input intermediate files (files are sorted by mapper)
            // Also, get smallest key line from each file and push into minHeap
            for (int i = 0; i < filesList.size(); i++) {
                BufferedReader reader = Files.newBufferedReader(Path.of(filesList.get(i).getFileName()));
                readers.add(reader);

                HeapEntry firstEntry = readEntry(reader, i);

                if(firstEntry != null)
                    minHeap.add(firstEntry);
            }

            // process each entry from the heap
            String currentKey = null;
            List<String> currentValues = new ArrayList<>();

            while(!minHeap.isEmpty()) {
                HeapEntry entry = minHeap.poll();

                // key changed, flushing current key and current values to reduce
                if(currentKey != null && !currentKey.equals(entry.key())) {
                    reducer.reduce(currentKey, currentValues, reducerContext);
                    currentValues.clear();
                }

                currentKey = entry.key();
                currentValues.add(entry.value()); // grouping values by key

                // reading next line from the file the entry came from and pushing to heap
                HeapEntry nextEntry = readEntry(readers.get(entry.fileIndex()), entry.fileIndex());

                if(nextEntry != null)
                    minHeap.add(nextEntry);
            }

            // flushing last group of values after heap becomes empty
            if(currentKey != null && !currentValues.isEmpty()) {
                reducer.reduce(currentKey, currentValues, reducerContext);
            }

        } catch (IOException e) {
            logger.severe("Unable to read file for reducer");
            throw e;
        } finally {
            //close all readers
            for(BufferedReader reader : readers)
                reader.close();
        }
    }
}
