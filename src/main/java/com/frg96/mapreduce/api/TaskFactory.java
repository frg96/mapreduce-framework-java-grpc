package com.frg96.mapreduce.api;

/**
 * Creates mapper and reducer instances for a registered application.
 *
 * <p>Factories are registered under an application ID in
 * {@link TaskRegistry}. Each call should return a new task instance so state
 * is not shared between independent RPCs.</p>
 */
public interface TaskFactory {
    /**
     * Creates a mapper for one mapper task.
     *
     * @return a new {@link Mapper} instance
     */
    Mapper createMapper();

    /**
     * Creates a reducer for one reducer task.
     *
     * @return a new {@link Reducer} instance
     */
    Reducer createReducer();
}
