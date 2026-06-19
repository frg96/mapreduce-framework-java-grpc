package com.frg96.mapreduce.api;

/**
 * Receives final records produced by a {@link Reducer}.
 *
 * <p>Records are buffered and written to the reducer's partition output
 * file. Keys and values must be non-null and should not contain whitespace
 * when output will be consumed as whitespace-delimited data.</p>
 */
public interface ReducerContext {
    /**
     * Emits a final key-value pair after reducing.
     *
     * @param key key to emit after reducing
     * @param value value to emit after reducing
     */
    void emit(String key, String value);
}
