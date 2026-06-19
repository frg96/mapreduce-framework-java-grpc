package com.frg96.mapreduce.core;

/**
 * Describes an input file available to mapper workers.
 *
 * @param filePath path used by workers to access the file
 * @param fileSizeBytes file size captured when the job configuration is parsed
 */
public record InputFile(
        String filePath,
        long fileSizeBytes
) {}
