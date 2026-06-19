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
 * Worker-side implementation of {@link ReducerContext}.
 *
 * <p>Reducer output is buffered in memory and periodically appended to a
 * partition output file. The buffer is flushed automatically when it
 * reaches the configured key-value pair threshold and once more after the
 * reducer finishes.</p>
 *
 * <p>This context is created for one reducer task and is not thread-safe.</p>
 */
class ReducerContextImpl implements ReducerContext {
    /**
     * Maximum number of buffered records before an automatic flush.
     */
    private static final int KEY_VALUE_PAIR_THRESHOLD = 3000;

    private final Path outputFile;

    private final List<Map.Entry<String, String>> keyValuePairs;

    /**
     * Creates a reducer context that writes to the given output file.
     *
     * @param outputFile destination for reducer output
     */
    ReducerContextImpl(Path outputFile) {
        this.outputFile = outputFile;
        this.keyValuePairs = new ArrayList<>();
    }

    /**
     * Buffers a key-value pair emitted by the reducer.
     *
     * <p>If the buffer has reached its threshold, existing records are flushed
     * before the new pair is added.</p>
     *
     * @param key key emitted by the reducer
     * @param value value associated with the key
     * @throws RuntimeException if an automatic flush fails
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

    /**
     * Appends the buffered reducer key-value pairs to the output file and
     * clears the in-memory buffer after a successful write.
     *
     * <p>The output file is created if it does not exist. Each key-value pair
     * is written on a separate line using a space as the delimiter.</p>
     *
     * @throws IOException if the output file cannot be opened or written
     */
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
