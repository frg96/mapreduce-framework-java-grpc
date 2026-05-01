package com.frg96.mapreduce.core;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Utility class for parsing the config.properties file.
 */
public class SpecParser {
    private SpecParser() {}

    /**
     * Parses the config.properties file and returns a MapReduceSpec object.
     * @param configFilePath path to the config.properties file
     * @return MapReduceSpec object
     */
    public static MapReduceSpec parse(String configFilePath) {
        Properties properties = new Properties();
        Path configPath = Paths.get(configFilePath);

        try(Reader reader = Files.newBufferedReader(configPath, StandardCharsets.UTF_8)) {
            properties.load(reader);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read config file: " + configPath, e);
        }

        int numWorkers = Integer.parseInt(requiredValue(properties, "num_workers"));

        List<String> workerAddresses = List.of(requiredValue(properties, "worker_addresses").split(","));

        List<InputFile> inputFiles = Arrays.stream(requiredValue(properties, "input_files").split(","))
                .map(SpecParser::toInputFile)
                .toList();

        String outputDir = requiredValue(properties, "output_dir");

        int numPartitions = Integer.parseInt(requiredValue(properties, "num_output_files"));

        int shardSizeKb = Integer.parseInt(requiredValue(properties, "map_kilobytes"));

        String appId = requiredValue(properties, "app_id");

        return new MapReduceSpec(numWorkers, workerAddresses, inputFiles, outputDir, numPartitions, shardSizeKb, appId);
    }

    private static String requiredValue(Properties properties, String key) {
        String value = properties.getProperty(key);

        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required property: " + key);
        }

        return value.strip();
    }

    private static InputFile toInputFile(String filePath) {
        Path path = Path.of(filePath);

        try {
            long fileSize = Files.size(path);
            return new InputFile(path.toString(), fileSize);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to read file size: " + path, e);
        }
    }
}
