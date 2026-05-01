package com.frg96.mapreduce.api;

/**
 * Reducer context.
 */
public interface ReducerContext {
    /**
     * Emits a key-value pair after reducing.
     * @param key key to emit after reducing
     * @param value value to emit after reducing
     */
    void emit(String key, String value);
}
