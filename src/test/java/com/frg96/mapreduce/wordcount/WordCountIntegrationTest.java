package com.frg96.mapreduce.wordcount;

import com.frg96.mapreduce.api.MapReduceJob;
import com.frg96.mapreduce.testutils.TestFixture;
import com.frg96.mapreduce.testutils.TestUtils;
import com.frg96.mapreduce.worker.Worker;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class WordCountIntegrationTest {
    private static final String APP_ID = "frg96-wordcount-test";

    private final List<String> workerAddresses = List.of(
            "localhost:50051",
            "localhost:50052",
            "localhost:50053",
            "localhost:50054",
            "localhost:50055",
            "localhost:50056"
    );

    private final List<Worker> workers = new ArrayList<>();

    @BeforeAll
    void startWorkers() {
        WordCountTasks.register(APP_ID);

        for (String address : workerAddresses) {
            Worker worker = new Worker(address);
            worker.start();
            workers.add(worker);
        }
    }

    @AfterAll
    void stopWorkers() throws InterruptedException {
        for (Worker worker : workers) {
            worker.stop();
        }
    }

    @Test
    @DisplayName("Word Count Intg Test for single small-sized Input File")
    void wordCountSmallSingleInput(@TempDir Path tempDir) throws Exception {
        TestFixture fixture = TestUtils.createTestFixture(
                tempDir,
                Path.of("src/test/resources/input_files/wordcount/input_small_single"),
                6,
                workerAddresses,
                List.of("testdata_1.txt"),
                "output",
                8,
                500,
                APP_ID
        );

        boolean success = new MapReduceJob().run(fixture.configFile().toString());
        assertTrue(success);

        Path actualOutputDir = fixture.outputDir();
        Path expectedOutputDir = Path.of("src/test/resources/expected_output_files/wordcount/output_small");

        // For manual checking outputDir:
        //TestUtils.copyDirectory(actualOutputDir, Path.of("src/test/resources/output"));

        Map<String, Integer> actualWordCountMap = TestUtils.getWordCountFromFiles(actualOutputDir.resolve("final"));
        Map<String, Integer> expectedWordCountMap = TestUtils.getWordCountFromFiles(expectedOutputDir);

        assertEquals(actualWordCountMap, expectedWordCountMap);

    }

    @Test
    @DisplayName("Word Count Intg Test for multiple medium-sized Input Files")
    void wordCountMediumInput(@TempDir Path tempDir) throws Exception {
        TestFixture fixture = TestUtils.createTestFixture(
                tempDir,
                Path.of("src/test/resources/input_files/wordcount/input_medium_multiple"),
                6,
                workerAddresses,
                List.of("testdata_1.txt","testdata_2.txt","testdata_3.txt"),
                "output",
                8,
                500,
                APP_ID
        );

        boolean success = new MapReduceJob().run(fixture.configFile().toString());
        assertTrue(success);

        Path actualOutputDir = fixture.outputDir();
        Path expectedOutputDir = Path.of("src/test/resources/expected_output_files/wordcount/output_medium");

        // For manual checking outputDir:
        //TestUtils.copyDirectory(actualOutputDir, Path.of("src/test/resources/output"));

        Map<String, Integer> actualWordCountMap = TestUtils.getWordCountFromFiles(actualOutputDir.resolve("final"));
        Map<String, Integer> expectedWordCountMap = TestUtils.getWordCountFromFiles(expectedOutputDir);

        assertEquals(actualWordCountMap, expectedWordCountMap);
    }

    @Test
    @DisplayName("Word Count Intg Test for single large-sized Input Files")
    void wordCountLargeSingleInput(@TempDir Path tempDir) throws Exception {
        TestFixture fixture = TestUtils.createTestFixture(
                tempDir,
                Path.of("src/test/resources/input_files/wordcount/input_large_single"),
                6,
                workerAddresses,
                List.of("testdata_1.txt"),
                "output",
                8,
                500,
                APP_ID
        );

        boolean success = new MapReduceJob().run(fixture.configFile().toString());
        assertTrue(success);

        Path actualOutputDir = fixture.outputDir();
        Path expectedOutputDir = Path.of("src/test/resources/expected_output_files/wordcount/output_large");

        // For manual checking outputDir:
        //TestUtils.copyDirectory(actualOutputDir, Path.of("src/test/resources/output"));

        Map<String, Integer> actualWordCountMap = TestUtils.getWordCountFromFiles(actualOutputDir.resolve("final"));
        Map<String, Integer> expectedWordCountMap = TestUtils.getWordCountFromFiles(expectedOutputDir);

        assertEquals(actualWordCountMap, expectedWordCountMap);
    }




}
