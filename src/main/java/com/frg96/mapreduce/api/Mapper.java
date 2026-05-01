package com.frg96.mapreduce.api;

/**
 * Mapper interface that must be implemented by the user.
 */
public interface Mapper {
    /**
     * Maps an input line to a key-value pair.
     * Use {@link MapperContext#emit(String, String)} to emit a key-value pair.
     * @param inputLine the input line to map
     * @param context the mapper context
     */
    void map(String inputLine, MapperContext context);
}
