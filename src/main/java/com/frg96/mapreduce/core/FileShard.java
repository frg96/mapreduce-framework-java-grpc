package com.frg96.mapreduce.core;

import java.util.ArrayList;
import java.util.List;

/**
 * Describes the portions of one or more input files assigned to a mapper task.
 *
 * <p>File names and offset ranges are parallel lists: entries at the same
 * index describe one file segment. Ranges are half-open, meaning
 * {@code startOffset} is inclusive and {@code endOffset} is exclusive.</p>
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
     * Adds the half-open byte range {@code [startOffset, endOffset)} of a file.
     *
     * @param fileName input file path
     * @param startOffset byte offset of the first byte in the file
     * @param endOffset byte offset of the last byte in the file
     */
    public void addFileAndOffsets(String fileName, long startOffset, long endOffset) {
        fileNames.add(fileName);
        offsetRanges.add(new OffsetRange(startOffset, endOffset));
    }

    /**
     * @return file paths included in this shard, in range order
     */
    public List<String> getFileNames() {
        return fileNames;
    }

    /**
     * @return byte ranges corresponding by index to {@link #getFileNames()}
     */
    public List<OffsetRange> getOffsetRanges() {
        return offsetRanges;
    }

    /**
     * @return {@code true} when the shard contains no file segments, {@code false} otherwise
     */
    public boolean isEmpty() {
        return fileNames.isEmpty();
    }


    /**
     * Represents a range of byte offsets for a file in the shard.
     *
     * @param startOffset starting inclusive byte offset in the range
     * @param endOffset ending exclusive byte offset in the range
     */
    public record OffsetRange(long startOffset, long endOffset) {}
}
