package com.frg96.mapreduce.apps;

import com.frg96.mapreduce.api.TaskFactory;
import com.frg96.mapreduce.api.TaskRegistry;

/**
 * Registers the MapReduce applications bundled with the framework.
 *
 * <p>Each worker JVM maintains its own {@link TaskRegistry}, so this
 * registration must run during worker startup before any tasks are
 * accepted. The application ID in the job configuration must match
 * an ID registered by this class.</p>
 */
public class BuiltInApps {
    private BuiltInApps() {}

    /**
     * Registers all built-in mapper and reducer factories.
     *
     * @throws IllegalStateException if an application ID is already registered
     */
    public static void registerAll() {
        register("wordcount", new WordCountTaskFactory());
    }

    /**
     * Registers a task factory under its application ID.
     *
     * @param appId unique identifier referenced by job configuration
     * @param factory factory used to create mapper and reducer instances
     * @throws IllegalStateException if {@code appId} is already registered
     */
    private static void register(String appId, TaskFactory factory) {
        if (!TaskRegistry.register(appId, factory)) {
            throw new IllegalStateException("Duplicate TaskFactory registration: " + appId);
        }
    }
}
