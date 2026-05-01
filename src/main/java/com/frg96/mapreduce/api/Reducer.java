package com.frg96.mapreduce.api;

import java.util.List;

/**
 * Reducer interface that must be implemented by the user.
 */
public interface Reducer {
    /**
     * Reduces a list of values for a given key.
     * Use {@link ReducerContext#emit(String, String)} to emit a key-value pair after reducing.
     * @param key the key to reduce
     * @param values the list of values to reduce
     * @param context the reducer context
     */
    void reduce(String key, List<String> values, ReducerContext context);
}
