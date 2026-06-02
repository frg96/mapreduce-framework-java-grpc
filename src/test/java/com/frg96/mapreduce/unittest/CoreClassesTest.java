package com.frg96.mapreduce.unittest;

import com.frg96.mapreduce.core.*;
import org.junit.jupiter.api.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class CoreClassesTest {

    @Test
    public void testPrintUserDir() {
        System.out.println(System.getProperty("user.dir")); // /Users/frg96/Dev/IdeaProjects/MapReduceFramework in IntelliJ
        assertTrue(true);
    }

    @Test
    public void testSpecParserAndValidator() {
        try {
            MapReduceSpec spec = SpecParser.parse("src/test/resources/config.properties");
            assertTrue(SpecValidator.validate(spec));
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testIntermediateDirectoryHandler() {
        try {
            assertTrue(IntermediateDirectoryHandler.prepareIntermediateDirectory("output"));
            assertTrue(Files.exists(Path.of("output")));
            assertTrue(Files.exists(Path.of("output", "intermediate")));
            assertTrue(Files.exists(Path.of("output", "intermediate", "mapper")));
            assertTrue(Files.exists(Path.of("output", "intermediate", "reducer")));
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testFileSharder() {
        MapReduceSpec spec = SpecParser.parse("src/test/resources/config.properties");
        List<FileShard> fileShards = FileSharder.shardFiles(spec);
        assertNotNull(fileShards);
    }
}
