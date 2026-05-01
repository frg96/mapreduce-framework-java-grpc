package com.frg96.mapreduce.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Task registry. Used to register mappers and reducers.
 * @see TaskFactory
 */
public class TaskRegistry {
    private static final Map<String, TaskFactory> factories = new ConcurrentHashMap<>();

    private TaskRegistry() {}

    /**
     * Registers a new TaskFactory for the given app_id.
     * @param appId the app_id
     * @param factory the TaskFactory object
     * @return true if the factory was registered successfully, false otherwise
     */
    public static boolean register(String appId, TaskFactory factory) {
        return factories.putIfAbsent(appId, factory) == null;
    }

    /**
     * Creates a new mapper for the given app_id using the registered TaskFactory.
     * @param appId the app_id
     * @return a new Mapper object
     */
    public static Mapper createMapper(String appId) {
        TaskFactory factory = factories.get(appId);

        if (factory == null) {
            throw new IllegalArgumentException("No mapper registered for app_id: " + appId);
        }

        return factory.createMapper();
    }

    /**
     * Creates a new reducer for the given app_id using the registered TaskFactory.
     * @param appId the app_id
     * @return a new Reducer object
     */
    public static Reducer createReducer(String appId) {
        TaskFactory factory = factories.get(appId);

        if (factory == null) {
            throw new IllegalArgumentException("No reducer registered for app_id: " + appId);
        }

        return factory.createReducer();
    }
}
