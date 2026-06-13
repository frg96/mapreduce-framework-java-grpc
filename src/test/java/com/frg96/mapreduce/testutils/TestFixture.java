package com.frg96.mapreduce.testutils;

import java.nio.file.Path;
import java.util.List;

public record TestFixture(
        Path configFile,
        List<Path> inputFiles,
        Path outputDir
) {}
