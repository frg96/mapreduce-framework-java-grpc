package com.frg96.mapreduce.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JVM-local, thread-safe registry of MapReduce applications.
 *
 * <p>Each application ID maps to one {@link TaskFactory}. Because static
 * state is not shared between JVMs, every standalone worker must initialize
 * its own registry before accepting requests.</p>
 *
 * @see TaskFactory
 */
public class TaskRegistry {
    private static final Map<String, TaskFactory> factories = new ConcurrentHashMap<>();

    private TaskRegistry() {}

    /**
     * Registers a factory unless the application ID already exists.
     *
     * @param appId unique application ID used by job configurations
     * @param factory factory associated with the application
     * @return {@code true} when registered; {@code false} when already present
     * @throws NullPointerException if either argument is {@code null}
     */
    public static boolean register(String appId, TaskFactory factory) {
        return factories.putIfAbsent(appId, factory) == null;
    }

    /**
     * Creates a mapper for a registered application.
     *
     * @param appId registered application ID
     * @return a new mapper instance
     * @throws IllegalArgumentException if the application is not registered
     */
    public static Mapper createMapper(String appId) {
        TaskFactory factory = factories.get(appId);

        if (factory == null) {
            throw new IllegalArgumentException("No mapper registered for app_id: " + appId);
        }

        return factory.createMapper();
    }

    /**
     * Creates a reducer for a registered application.
     *
     * @param appId registered application ID
     * @return a new reducer instance
     * @throws IllegalArgumentException if the application is not registered
     */
    public static Reducer createReducer(String appId) {
        TaskFactory factory = factories.get(appId);

        if (factory == null) {
            throw new IllegalArgumentException("No reducer registered for app_id: " + appId);
        }

        return factory.createReducer();
    }
}
