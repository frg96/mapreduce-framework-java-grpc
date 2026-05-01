package com.frg96.mapreduce.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a shard for Mapper input consisting of one or more files.
 */
public class FileShard {
    /**
     * List of file names in the shard.
     */
    private final List<String> fileNames = new ArrayList<>();

    /**
     * List of byte offset ranges for each file in the shard.
     */
    private final List<OffsetRange> offsetRanges = new ArrayList<>();

    /**
     * Adds a file and its byte offset range to the shard.
     * @param fileName input file path
     * @param startOffset byte offset of the first byte in the file
     * @param endOffset byte offset of the last byte in the file
     */
    public void addFileAndOffsets(String fileName, long startOffset, long endOffset) {
        fileNames.add(fileName);
        offsetRanges.add(new OffsetRange(startOffset, endOffset));
    }

    /**
     * Returns the list of file names in the shard.
     * @return list of file names
     */
    public List<String> getFileNames() {
        return fileNames;
    }

    /**
     * Returns the list of byte offset ranges for each file in the shard.
     * @return list of byte offset ranges
     */
    public List<OffsetRange> getOffsetRanges() {
        return offsetRanges;
    }

    /**
     * Checks if the shard is empty (contains no files).
     * @return true if the shard is empty, false otherwise
     */
    public boolean isEmpty() {
        return fileNames.isEmpty();
    }


    /**
     * Represents a range of byte offsets for a file in the shard.
     * @param startOffset starting byte offset in the range
     * @param endOffset ending byte offset in the range
     */
    public record OffsetRange(long startOffset, long endOffset) {}
}
