package com.frg96.mapreduce.testutils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

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
                    joinAddressStrings(workerAddresses),
                    joinPaths(inputFilesPaths),
                    outputDirPath.toString(),
                    numPartitions,
                    shardSizeKb,
                    appId
                )
        );



        return new TestFixture(configFilePath, inputFilesPaths, outputDirPath);
    }

    private static String joinAddressStrings(List<String> addresses) {
        return addresses.stream()
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
    }


    private static String joinPaths(List<Path> paths) {
        return paths.stream()
                .map(Path::toString)
                .reduce((left, right) -> left + "," + right)
                .orElseThrow();
    }


}
