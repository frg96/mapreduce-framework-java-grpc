package com.frg96.mapreduce.api;

import java.util.List;

/**
 * User-defined aggregation applied once to each distinct intermediate key.
 *
 * <p>Implementations emit final records through the supplied
 * {@link ReducerContext}. A new reducer is created for each reducer RPC.</p>
 */
public interface Reducer {
    /**
     * Processes and reduces all intermediate values associated with one key.
     *
     * <p>The values list is valid only for the duration of this call and should
     *  be treated as read-only.</br>
     * Use {@link ReducerContext#emit(String, String)} to emit a key-value pair after reducing.</p>
     *
     * @param key intermediate grouping key
     * @param values values emitted for the key
     * @param context the reducer context used to emit final records
     */
    void reduce(String key, List<String> values, ReducerContext context);
}
