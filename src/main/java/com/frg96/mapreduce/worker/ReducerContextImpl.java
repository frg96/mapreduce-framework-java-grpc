package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.api.ReducerContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of {@link ReducerContext} interface.
 * Buffers reducer output and flushes final files.
 */
class ReducerContextImpl implements ReducerContext {
    private static final int KEY_VALUE_PAIR_THRESHOLD = 3000;

    private final Path outputFile;

    private final List<Map.Entry<String, String>> keyValuePairs;

    ReducerContextImpl(Path outputFile) {
        this.outputFile = outputFile;
        this.keyValuePairs = new ArrayList<>();
    }

    /**
     * Emits a key-value pair after reducing.
     *
     * @param key   key to emit after reducing
     * @param value value to emit after reducing
     */
    @Override
    public void emit(String key, String value) {
        if(this.keyValuePairs.size() >= KEY_VALUE_PAIR_THRESHOLD) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        this.keyValuePairs.add(Map.entry(key, value));
    }

    void flush() throws IOException {
        try(BufferedWriter writer = Files.newBufferedWriter(
                this.outputFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        )) {
            for(Map.Entry<String, String> entry : this.keyValuePairs) {
                writer.write(entry.getKey());
                writer.write(" ");
                writer.write(entry.getValue());
                writer.newLine();
            }
        }

        this.keyValuePairs.clear();
    }
}
