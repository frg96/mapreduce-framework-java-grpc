package com.frg96.mapreduce.api;

/**
 * Mapper context.
 */
public interface MapperContext {
    /**
     * Emits a key-value pair after mapping.
     * @param key key to emit after mapping
     * @param value value to emit after mapping
     */
    void emit(String key, String value);
}
