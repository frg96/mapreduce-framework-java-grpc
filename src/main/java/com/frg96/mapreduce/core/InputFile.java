package com.frg96.mapreduce.core;

/**
 * Record representing a Mapper input file.
 * @param filePath
 * @param fileSizeBytes
 */
public record InputFile(
        String filePath,
        long fileSizeBytes
) {}
