package com.frg96.mapreduce.api;

/**
 * User-defined transformation applied to each input line of a map task.
 *
 * <p>Implementations emit zero or more intermediate records through the
 * supplied {@link MapperContext}. A new mapper is created for each mapper
 * RPC, so implementations may keep task-local state.</p>
 */
public interface Mapper {
    /**
     * Processes and maps one input line to a key-value pair.
     *
     * <p>Use {@link MapperContext#emit(String, String)} to emit a key-value pair.</p>
     *
     * @param inputLine input record without its line terminator
     * @param context the mapper context used to emit intermediate records
     */
    void map(String inputLine, MapperContext context);
}
