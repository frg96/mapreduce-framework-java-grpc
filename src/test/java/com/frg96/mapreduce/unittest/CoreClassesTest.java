package com.frg96.mapreduce.unittest;

import com.frg96.mapreduce.core.*;
import com.frg96.mapreduce.testutils.TestFixture;
import com.frg96.mapreduce.testutils.TestUtils;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CoreClassesTest {

    @Test
    public void testSpecParserAndValidator(@TempDir Path tempDir) throws IOException {
        TestFixture testFixture = TestUtils.createTestFixture(
                tempDir,
                Path.of("src/test/resources/input_files/wordcount/input_medium_multiple"),
                6,
                List.of("localhost:50051","localhost:50052","localhost:50053","localhost:50054","localhost:50055","localhost:50056"),
                List.of("testdata_1.txt", "testdata_2.txt", "testdata_3.txt"),
                "output",
                8,
                500,
                "frg96-coretest"
        );

        MapReduceSpec spec = SpecParser.parse(testFixture.configFile().toString());

        assertEquals(6, spec.numWorkers());
        assertEquals(List.of("localhost:50051","localhost:50052","localhost:50053","localhost:50054","localhost:50055","localhost:50056"), spec.workerAddresses());
        assertEquals(3, spec.inputFiles().size());
        assertEquals(testFixture.outputDir().toString(), spec.outputDir());
        assertEquals(8, spec.numPartitions());
        assertEquals(500, spec.shardSizeKb());
        assertEquals("frg96-coretest", spec.appId());

        assertTrue(SpecValidator.validate(spec));
    }

    @Test
    void testSpecParserMissingRequiredProperty(@TempDir Path tempDir) throws IOException {
        Path config = tempDir.resolve("config.properties");

        Files.writeString(config, """
            num_workers=1
            worker_addresses=localhost:50051
            output_dir=%s
            num_output_files=8
            map_kilobytes=500
            app_id=frg96-coretest
            """.formatted(tempDir));

        IllegalArgumentException e = assertThrows(
                IllegalArgumentException.class,
                () -> SpecParser.parse(config.toString())
        );

        assertTrue(e.getMessage().contains("Missing required property: input_files"));
    }

    @Test
    void testSpecParserInvalidNumber(@TempDir Path tempDir) throws IOException {
        Path input = tempDir.resolve("input.txt");
        Files.writeString(input, "hello\n");

        Path output = tempDir.resolve("output");
        Files.createDirectories(output);

        Path config = tempDir.resolve("config.properties");
        Files.writeString(config, """
            num_workers=abc
            worker_addresses=localhost:50051
            input_files=%s
            output_dir=%s
            num_output_files=1
            map_kilobytes=1
            app_id=test
            """.formatted(input, output));

        assertThrows(NumberFormatException.class, () -> SpecParser.parse(config.toString()));
    }

    @Test
    void testSpecValidatorRejectsWorkerCountMismatch(@TempDir Path tempDir) throws IOException {
        TestFixture fixture = TestUtils.createTestFixture(
                tempDir,
                Path.of("src/test/resources/input_files/wordcount/input_small_single"),
                2,
                List.of("localhost:50051"),
                List.of("testdata_1.txt"),
                "output",
                1,
                1,
                "test"
        );

        MapReduceSpec spec = SpecParser.parse(fixture.configFile().toString());

        assertFalse(SpecValidator.validate(spec));
    }

    @Test
    public void testIntermediateDirectoryHandlerDeletesOldMapperAndReducerFiles(@TempDir Path tempDir) throws IOException {
        Path outputDir = tempDir.resolve("output");

        Path oldMapperFile = outputDir.resolve("intermediate/mapper/old-map.txt");
        Path oldReducerFile = outputDir.resolve("intermediate/reducer/old-reduce.txt");

        Files.createDirectories(oldMapperFile.getParent());
        Files.createDirectories(oldReducerFile.getParent());

        Files.writeString(oldMapperFile, "old mapper data");
        Files.writeString(oldReducerFile, "old reducer data");

        assertTrue(IntermediateDirectoryHandler.prepareIntermediateDirectory(outputDir.toString()));

        assertTrue(Files.exists(outputDir));
        assertTrue(Files.exists(outputDir.resolve( "intermediate")));
        assertTrue(Files.exists(outputDir.resolve("intermediate/mapper")));
        assertTrue(Files.exists(outputDir.resolve("intermediate/reducer")));
        assertTrue(Files.isDirectory(outputDir.resolve("intermediate/mapper")));
        assertTrue(Files.isDirectory(outputDir.resolve("intermediate/reducer")));

        assertFalse(Files.exists(oldMapperFile));
        assertFalse(Files.exists(oldReducerFile));
    }

    @Test
    public void testFileSharder(@TempDir Path tempDir) throws IOException {
        TestFixture testFixture = TestUtils.createTestFixture(
                tempDir,
                Path.of("src/test/resources/input_files/wordcount/input_medium_multiple"),
                6,
                List.of("localhost:50051","localhost:50052","localhost:50053","localhost:50054","localhost:50055","localhost:50056"),
                List.of("testdata_1.txt", "testdata_2.txt", "testdata_3.txt"),
                "output",
                8,
                500,
                "frg96-coretest"
        );

        MapReduceSpec spec = SpecParser.parse(testFixture.configFile().toString());
        List<FileShard> fileShards = FileSharder.shardFiles(spec);
        assertNotNull(fileShards);
        assertFalse(fileShards.isEmpty());

        assertEquals(2, fileShards.size());
        
        for (FileShard shard : fileShards) {
            assertFalse(shard.isEmpty());
            assertEquals(shard.getFileNames().size(), shard.getOffsetRanges().size());
        }
    }

    @Test
    void testFileSharderEndsShardAtNewline(@TempDir Path tempDir) throws IOException {
        Path inputDir = tempDir.resolve("input");
        Files.createDirectories(inputDir);

        Path input = inputDir.resolve("input.txt");
        Files.writeString(input, "aaaa\nbbbb\ncccc\n");

        TestFixture fixture = TestUtils.createTestFixture(
                tempDir,
                inputDir,
                1,
                List.of("localhost:50051"),
                List.of("input.txt"),
                "output",
                1,
                1,
                "test"
        );

        MapReduceSpec spec = SpecParser.parse(fixture.configFile().toString());
        List<FileShard> shards = FileSharder.shardFiles(spec);

        assertEquals(1, shards.size());
        assertEquals(0, shards.getFirst().getOffsetRanges().getFirst().startOffset());
        assertEquals(Files.size(input), shards.getFirst().getOffsetRanges().getFirst().endOffset());
    }
}
