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

class IntermediateMerger {
    private static final Logger logger = Logger.getLogger(IntermediateMerger.class.getName());

    private static record HeapEntry(String key, String value, int fileIndex) implements Comparable<HeapEntry>{
        @Override
        public int compareTo(HeapEntry o) {
            return this.key().compareTo(o.key());
        }
    }

    private static HeapEntry readEntry(BufferedReader reader, int fileIndex) throws IOException {
        String line = reader.readLine();

        if(line == null)
            return null;

        String[] lineSplit = line.split("\\s+");
        String key = lineSplit[0];
        String value = lineSplit[1];

        return new HeapEntry(key, value, fileIndex);
    }

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
                if(currentKey != null && currentKey.equals(entry.key())) {
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
