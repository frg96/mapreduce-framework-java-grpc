package com.frg96.mapreduce.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Divides configured input files into line-aligned mapper shards.
 *
 * <p>Shards target the configured byte size but may extend past it to avoid
 * splitting a line. A shard may contain ranges from multiple input files.</p>
 */
public class FileSharder {
    private static final Logger LOGGER = Logger.getLogger(FileSharder.class.getName());

    private FileSharder() {}

    /**
     * Creates mapper shards for the input files in a validated job specification.
     *
     * <p>Shards the input files into FileShards based on the shard size specified in the spec.</p>
     *
     * @param spec job specification containing input files and target shard size
     * @return list of {@link FileShard} objects in input-file and byte-offset order
     * @throws IllegalArgumentException if an input file cannot be read
     */
    public static List<FileShard> shardFiles(MapReduceSpec spec) {
        List<FileShard> fileShards = new ArrayList<>();


        int numInputFiles = spec.inputFiles().size();
        LOGGER.info("Number of input files: " + numInputFiles);

        long totalFilesSizeBytes = 0L;
        for(InputFile inputFile: spec.inputFiles()) {
            totalFilesSizeBytes += inputFile.fileSizeBytes();
        }
        LOGGER.info("Total size of input files: " + totalFilesSizeBytes + " bytes");

        long shardSizeBytes = spec.shardSizeKb() * 1024L;

        int numShards = (int) Math.ceil((double) totalFilesSizeBytes / shardSizeBytes);
        LOGGER.info("Number of shards: " + numShards);

        FileShard currentShard = new FileShard();
        long remainingShardSizeBytes = shardSizeBytes;

        for(InputFile inputFile: spec.inputFiles()) {
            long fileSizeBytes = inputFile.fileSizeBytes();

            long remainingFileSizeBytes = fileSizeBytes;

            long fileOffset = 0L;

            while(remainingFileSizeBytes > 0) {
                // remaining shard is big enough to fit remaining file
                if(remainingShardSizeBytes >= remainingFileSizeBytes){
                    currentShard.addFileAndOffsets(inputFile.filePath(), fileOffset, fileOffset + remainingFileSizeBytes);
                    remainingShardSizeBytes -= remainingFileSizeBytes;
                    remainingFileSizeBytes = 0; // exit out of while
                }
                // remaining shard is not big enough. So partial file must be fit into shard
                else {
                    long usedSizeBytes = remainingShardSizeBytes;

                    try (RandomAccessFile file = new RandomAccessFile(inputFile.filePath(), "r")) {
                        long offset = fileOffset + remainingShardSizeBytes;
                        file.seek(offset);

                        int nextByte;
                        while((nextByte = file.read()) != -1) {
                            usedSizeBytes++;

                            if(nextByte == '\n'){
                                break;
                            }
                        }
                    } catch (IOException e) {
                        throw new IllegalArgumentException("Failed to shard file: " + inputFile.filePath(), e);
                    }

                    currentShard.addFileAndOffsets(inputFile.filePath(), fileOffset, fileOffset + usedSizeBytes);

                    // add current shard to list and start new one for remaining file
                    fileShards.add(currentShard);

                    currentShard = new FileShard();

                    remainingShardSizeBytes = shardSizeBytes;
                    remainingFileSizeBytes -= usedSizeBytes;
                    fileOffset += usedSizeBytes;

                }
            } // end of while
        } // end of InputFile iteration loop

        // add last shard
        if(!currentShard.getFileNames().isEmpty()) {
            fileShards.add(currentShard);
        }

        LOGGER.log(Level.INFO, "Created {0} input shards", fileShards.size());

        for(int i = 0; i < fileShards.size(); i++) {
            FileShard fileShard = fileShards.get(i);

            for(int j = 0; j < fileShard.getFileNames().size(); j++) {
                FileShard.OffsetRange range = fileShard.getOffsetRanges().get(j);

                LOGGER.log(
                        Level.INFO,
                        "Shard {0}, segment {1}: {2} [{3}, {4})",
                        new Object[]{
                                i,
                                j,
                                fileShard.getFileNames().get(j),
                                range.startOffset(),
                                range.endOffset()
                        }
                );
            }
        }

        return fileShards;
    }
}
