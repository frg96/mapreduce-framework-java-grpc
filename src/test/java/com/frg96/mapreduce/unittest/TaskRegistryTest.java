package com.frg96.mapreduce.unittest;

import com.frg96.mapreduce.api.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class TaskRegistryTest {
    @Test
    void testRegisterAndCreateMapper() {
        String appId = uniqueAppId();

        boolean registered = TaskRegistry.register(appId, new TestTaskFactory());

        assertTrue(registered);
        assertInstanceOf(TestMapper.class, TaskRegistry.createMapper(appId));
    }

    @Test
    void testRegisterAndCreateReducer() {
        String appId = uniqueAppId();

        boolean registered = TaskRegistry.register(appId, new TestTaskFactory());

        assertTrue(registered);
        assertInstanceOf(TestReducer.class, TaskRegistry.createReducer(appId));
    }

    @Test
    void testDuplicateRegistrationReturnsFalse() {
        String appId = uniqueAppId();

        boolean firstRegistration = TaskRegistry.register(appId, new TestTaskFactory());
        boolean secondRegistration = TaskRegistry.register(appId, new TestTaskFactory());

        assertTrue(firstRegistration);
        assertFalse(secondRegistration);
    }

    @Test
    void testCreateMapperForUnknownAppIdThrows() {
        String appId = uniqueAppId();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> TaskRegistry.createMapper(appId)
        );

        assertTrue(exception.getMessage().contains("No mapper registered for app_id"));
    }

    @Test
    void testCreateReducerForUnknownAppIdThrows() {
        String appId = uniqueAppId();

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> TaskRegistry.createReducer(appId)
        );

        assertTrue(exception.getMessage().contains("No reducer registered for app_id"));
    }


    private static String uniqueAppId() {
        return "task-registry-test-" + UUID.randomUUID();
    }


    private static class TestTaskFactory implements TaskFactory {
        @Override
        public Mapper createMapper() {
            return new TestMapper();
        }

        @Override
        public Reducer createReducer() {
            return new TestReducer();
        }
    }

    private static class TestMapper implements Mapper {
        @Override
        public void map(String inputLine, MapperContext context) {
            context.emit(inputLine, "1");
        }
    }

    private static class TestReducer implements Reducer {
        @Override
        public void reduce(String key, List<String> values, ReducerContext context) {
            context.emit(key, Integer.toString(values.size()));
        }
    }
}
