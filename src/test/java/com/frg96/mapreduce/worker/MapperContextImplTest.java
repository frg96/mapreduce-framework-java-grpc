package com.frg96.mapreduce.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class MapperContextImplTest {

    @Test
    void testEmitAndFlushWritesToPartitionFiles(@TempDir Path tempDir) throws Exception {
        List<Path> intermediateFiles = List.of(
                tempDir.resolve("part-0"),
                tempDir.resolve("part-1"),
                tempDir.resolve("part-2")
        );

        MapperContextImpl context = new MapperContextImpl(3, intermediateFiles);

        context.emit("apple", "1");
        context.emit("banana", "1");
        context.emit("apple", "1");

        context.flush();

        List<String> allLines = readAllLines(intermediateFiles);

        assertTrue(allLines.contains("apple 1"));
        assertTrue(allLines.contains("banana 1"));

        long appleCount = allLines.stream()
                .filter(line -> line.equals("apple 1"))
                .count();

        assertEquals(2, appleCount);
    }

    @Test
    void testSameKeyAlwaysGoesToSamePartition(@TempDir Path tempDir) throws Exception {
        List<Path> intermediateFiles = List.of(
                tempDir.resolve("part-0"),
                tempDir.resolve("part-1"),
                tempDir.resolve("part-2"),
                tempDir.resolve("part-3")
        );

        MapperContextImpl context = new MapperContextImpl(4, intermediateFiles);

        context.emit("same-key", "1");
        context.emit("same-key", "2");
        context.emit("same-key", "3");

        context.flush();

        int filesContainingKey = 0;

        for (Path file : intermediateFiles) {
            if (Files.exists(file)) {
                List<String> lines = Files.readAllLines(file);

                boolean containsSameKey = lines.stream()
                        .anyMatch(line -> line.startsWith("same-key "));

                if (containsSameKey) {
                    filesContainingKey++;
                    assertEquals(List.of(
                            "same-key 1",
                            "same-key 2",
                            "same-key 3"
                    ), lines);
                }
            }
        }

        assertEquals(1, filesContainingKey);
    }

    @Test
    void testFlushSortsKeysWithinEachPartition(@TempDir Path tempDir) throws Exception {
        List<Path> intermediateFiles = List.of(
                tempDir.resolve("part-0"),
                tempDir.resolve("part-1")
        );

        MapperContextImpl context = new MapperContextImpl(2, intermediateFiles);

        context.emit("zebra", "1");
        context.emit("apple", "1");
        context.emit("monkey", "1");
        context.emit("banana", "1");

        context.flush();

        for (Path file : intermediateFiles) {
            if (!Files.exists(file)) {
                continue;
            }

            List<String> lines = Files.readAllLines(file);
            List<String> sortedLines = lines.stream().sorted().toList();

            assertEquals(sortedLines, lines);
        }
    }

    @Test
    void testFlushCreatesEmptyPartitionFiles(@TempDir Path tempDir) throws Exception {
        List<Path> intermediateFiles = List.of(
                tempDir.resolve("part-0"),
                tempDir.resolve("part-1")
        );

        MapperContextImpl context = new MapperContextImpl(2, intermediateFiles);

        context.flush();

        assertTrue(Files.exists(intermediateFiles.get(0)));
        assertTrue(Files.exists(intermediateFiles.get(1)));

        assertTrue(Files.readAllLines(intermediateFiles.get(0)).isEmpty());
        assertTrue(Files.readAllLines(intermediateFiles.get(1)).isEmpty());
    }

    private static List<String> readAllLines(List<Path> files) throws Exception {
        return files.stream()
                .filter(Files::exists)
                .flatMap(file -> {
                    try {
                        return Files.readAllLines(file).stream();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();
    }
}