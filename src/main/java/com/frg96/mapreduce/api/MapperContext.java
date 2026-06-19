package com.frg96.mapreduce.api;

/**
 * Receives intermediate records produced by a {@link Mapper}.
 *
 * <p>Records are partitioned by key and later grouped for reduction. Keys
 * and values must be non-null and must not contain whitespace because the
 * current intermediate-file format uses whitespace delimiters.</p>
 */
public interface MapperContext {
    /**
     * Emits an intermediate key-value pair after mapping.
     *
     * @param key key to emit after mapping
     * @param value value to emit after mapping
     */
    void emit(String key, String value);
}
