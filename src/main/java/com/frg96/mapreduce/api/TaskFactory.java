package com.frg96.mapreduce.api;

/**
 * Factory interface for creating mappers and reducers.
 */
public interface TaskFactory {
    /**
     * Creates a new mapper.
     * @return object implementing {@link Mapper}
     */
    Mapper createMapper();

    /**
     * Creates a new reducer.
     * @return object implementing {@link Reducer}
     */
    Reducer createReducer();
}
