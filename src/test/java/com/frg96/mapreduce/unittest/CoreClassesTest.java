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

        try {
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
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testIntermediateDirectoryHandler(@TempDir Path tempDir) {
        try {
            Path outputDir = tempDir.resolve("output");

            Path oldMapperFile = outputDir.resolve("intermediate/mapper/old.txt");
            Files.createDirectories(oldMapperFile.getParent());
            Files.writeString(oldMapperFile, "old data");

            assertTrue(IntermediateDirectoryHandler.prepareIntermediateDirectory(outputDir.toString()));
            assertTrue(Files.exists(outputDir));
            assertTrue(Files.exists(outputDir.resolve( "intermediate")));
            assertTrue(Files.exists(outputDir.resolve("intermediate/mapper")));
            assertTrue(Files.exists(outputDir.resolve("intermediate/reducer")));

            assertFalse(Files.exists(oldMapperFile));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
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
}
