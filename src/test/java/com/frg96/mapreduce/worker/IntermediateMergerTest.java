package com.frg96.mapreduce.worker;

import com.frg96.mapreduce.File;
import com.frg96.mapreduce.api.Reducer;
import com.frg96.mapreduce.api.ReducerContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class IntermediateMergerTest {

    @Test
    void testMergeAndReduceGroupsSameKeyAcrossFiles(@TempDir Path tempDir) throws Exception {
        Path file1 = tempDir.resolve("part-1");
        Path file2 = tempDir.resolve("part-2");

        Files.writeString(file1, """
                apple 1
                banana 1
                carrot 1
                """);

        Files.writeString(file2, """
                apple 1
                banana 1
                banana 1
                dog 1
                """);

        Path outputFile = tempDir.resolve("reducer-output");
        ReducerContext context = new ReducerContextImpl(outputFile);

        Reducer countingReducer = (key, values, reducerContext) ->
                reducerContext.emit(key, Integer.toString(values.size()));

        IntermediateMerger.mergeAndReduce(
                countingReducer,
                List.of(
                        File.newBuilder().setFileName(file1.toString()).build(),
                        File.newBuilder().setFileName(file2.toString()).build()
                ),
                context
        );

        ((ReducerContextImpl) context).flush();

        List<String> lines = Files.readAllLines(outputFile);

        assertEquals(List.of(
                "apple 2",
                "banana 3",
                "carrot 1",
                "dog 1"
        ), lines);
    }

    @Test
    void testMergeAndReduceHandlesEmptyInputFile(@TempDir Path tempDir) throws Exception {
        Path emptyFile = tempDir.resolve("empty");
        Path dataFile = tempDir.resolve("data");

        Files.writeString(emptyFile, "");

        Files.writeString(dataFile, """
                apple 1
                banana 1
                """);

        Path outputFile = tempDir.resolve("reducer-output");
        ReducerContext context = new ReducerContextImpl(outputFile);

        Reducer countingReducer = (key, values, reducerContext) ->
                reducerContext.emit(key, Integer.toString(values.size()));

        IntermediateMerger.mergeAndReduce(
                countingReducer,
                List.of(
                        File.newBuilder().setFileName(emptyFile.toString()).build(),
                        File.newBuilder().setFileName(dataFile.toString()).build()
                ),
                context
        );

        ((ReducerContextImpl) context).flush();

        List<String> lines = Files.readAllLines(outputFile);

        assertEquals(List.of(
                "apple 1",
                "banana 1"
        ), lines);
    }

    @Test
    void testMergeAndReduceWithNoFilesProducesNoOutput(@TempDir Path tempDir) throws Exception {
        Path outputFile = tempDir.resolve("reducer-output");
        ReducerContextImpl context = new ReducerContextImpl(outputFile);

        Reducer countingReducer = (key, values, reducerContext) ->
                reducerContext.emit(key, Integer.toString(values.size()));

        IntermediateMerger.mergeAndReduce(
                countingReducer,
                List.of(),
                context
        );

        context.flush();

        assertTrue(Files.exists(outputFile));
        assertTrue(Files.readAllLines(outputFile).isEmpty());
    }
}