package com.frg96.mapreduce.apps;

import com.frg96.mapreduce.api.TaskFactory;
import com.frg96.mapreduce.api.TaskRegistry;

public class BuiltInApps {
    private BuiltInApps() {}

    public static void registerAll() {
        register("wordcount", new WordCountTaskFactory());
    }

    private static void register(String appId, TaskFactory factory) {
        if (!TaskRegistry.register(appId, factory)) {
            throw new IllegalStateException(
                    "Duplicate TaskFactory registration: " + appId
            );
        }
    }
}
