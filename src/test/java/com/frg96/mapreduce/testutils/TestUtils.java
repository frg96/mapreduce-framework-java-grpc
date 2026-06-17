package com.frg96.mapreduce.testutils;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {
    public static TestFixture createTestFixture(
            Path tempDir,
            Path inputDir,
            int numWorkers,
            List<String> workerAddresses,
            List<String> inputFiles,
            String outputDirName,
            int numPartitions,
            int shardSizeKb,
            String appId) throws IOException {

        Path configFilePath = tempDir.resolve("config.properties");

        List<Path> inputFilesPaths = inputFiles.stream()
                        .map(inputDir::resolve)
                        .toList();

        Path outputDirPath = tempDir.resolve(outputDirName);

        Files.createDirectories(outputDirPath);

        Files.writeString(configFilePath, """
                num_workers=%d
                worker_addresses=%s
                input_files=%s
                output_dir=%s
                num_output_files=%d
                map_kilobytes=%d
                app_id=%s
                """.formatted(
                    numWorkers,
                    String.join(",", workerAddresses),
                    joinPaths(inputFilesPaths),
                    outputDirPath.toString(),
                    numPartitions,
                    shardSizeKb,
                    appId
                )
        );

        return new TestFixture(configFilePath, inputFilesPaths, outputDirPath);
    }

    public static String joinPaths(List<Path> paths) {
        return paths.stream()
                .map(Path::toString)
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
    }

    public static void copyDirectory(Path sourceDir, Path destinationDir) throws IOException {
        // Delete destination if it already exists
        if (Files.exists(destinationDir)) {
            deleteRecursively(destinationDir);
        }

        Files.walkFileTree(sourceDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Path targetDir = destinationDir.resolve(sourceDir.relativize(dir));
                Files.createDirectories(targetDir);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Path targetFile = destinationDir.resolve(sourceDir.relativize(file));
                Files.copy(file, targetFile, StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static void deleteRecursively(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file,
                                             BasicFileAttributes attrs)
                    throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir,
                                                      IOException exc)
                    throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static Map<String, Integer> getWordCountFromFiles(Path directory) throws IOException {
        Map<String, Integer> wordCount = new TreeMap<>();

        try(Stream<Path> paths =  Files.walk(directory)) {
            for(Path file: paths.filter(Files::isRegularFile).toList()) {
                mergeWordCountFile(wordCount, file);
            }
        }

        return wordCount;
    }

    private static void mergeWordCountFile(Map<String, Integer> counts, Path file) throws IOException {
        for(String line: Files.readAllLines(file)) {
            if(line.isBlank())
                continue;

            String[] parts = line.split("\\s+");
            assertEquals(2, parts.length, "Invalid output line in " + file + ": " + line);

            counts.put(parts[0], Integer.parseInt(parts[1]));
        }
    }


}
