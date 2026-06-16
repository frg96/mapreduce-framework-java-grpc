package com.frg96.mapreduce.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ReducerContextImplTest {

    @Test
    void testEmitAndFlushWritesOutputFile(@TempDir Path tempDir) throws Exception {
        Path outputFile = tempDir.resolve("reducer-output");

        ReducerContextImpl context = new ReducerContextImpl(outputFile);

        context.emit("apple", "2");
        context.emit("banana", "1");

        context.flush();

        List<String> lines = Files.readAllLines(outputFile);

        assertEquals(List.of(
                "apple 2",
                "banana 1"
        ), lines);
    }

    @Test
    void testFlushWithNoEmitsCreatesEmptyFile(@TempDir Path tempDir) throws Exception {
        Path outputFile = tempDir.resolve("reducer-output");

        ReducerContextImpl context = new ReducerContextImpl(outputFile);

        context.flush();

        assertTrue(Files.exists(outputFile));
        assertTrue(Files.readAllLines(outputFile).isEmpty());
    }

    @Test
    void testMultipleFlushesAppendOnlyNewBufferedRecords(@TempDir Path tempDir) throws Exception {
        Path outputFile = tempDir.resolve("reducer-output");

        ReducerContextImpl context = new ReducerContextImpl(outputFile);

        context.emit("apple", "2");
        context.flush();

        context.emit("banana", "1");
        context.flush();

        List<String> lines = Files.readAllLines(outputFile);

        assertEquals(List.of(
                "apple 2",
                "banana 1"
        ), lines);
    }

    @Test
    void testAutomaticFlushAtThreshold(@TempDir Path tempDir) throws Exception {
        Path outputFile = tempDir.resolve("reducer-output");

        ReducerContextImpl context = new ReducerContextImpl(outputFile);

        for (int i = 0; i < 3001; i++) {
            context.emit("key-" + i, Integer.toString(i));
        }

        context.flush();

        List<String> lines = Files.readAllLines(outputFile);

        assertEquals(3001, lines.size());
        assertEquals("key-0 0", lines.get(0));
        assertEquals("key-3000 3000", lines.get(3000));
    }
}