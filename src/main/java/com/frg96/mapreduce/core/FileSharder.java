package com.frg96.mapreduce.core;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for sharding input files.
 */
public class FileSharder {
    private FileSharder() {}

    /**
     * Shards the input files into FileShards based on the shard size specified in the spec.
     * @param spec the MapReduceSpec object
     * @return a list of FileShard objects
     */
    public static List<FileShard> shardFiles(MapReduceSpec spec) {
        List<FileShard> fileShards = new ArrayList<>();


        int numInputFiles = spec.inputFiles().size();
        System.out.println("Number of input files: " + numInputFiles);

        long totatFilesSizeBytes = 0L;
        for(InputFile inputFile: spec.inputFiles()) {
            totatFilesSizeBytes += inputFile.fileSizeBytes();
        }
        System.out.println("Total size of input files: " + totatFilesSizeBytes + " bytes");

        long shardSizeBytes = spec.shardSizeKb() * 1024L;

        int numShards = (int) Math.ceil((double) totatFilesSizeBytes / shardSizeBytes);
        System.out.println("Number of shards: " + numShards);

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

        // DEBUG
        System.out.println("Size of fileShards: " + fileShards.size());

        //
        for(int i = 0; i < fileShards.size(); i++) {
            System.out.println("Shard no.: " + i);
            FileShard fileShard = fileShards.get(i);

            for(int j = 0; j < fileShard.getFileNames().size(); j++) {
                System.out.println("File name index: " + j + " " + fileShard.getFileNames().get(j) + " <" + fileShard.getOffsetRanges().get(j).startOffset() + ", " + fileShard.getOffsetRanges().get(j).endOffset() + ">");
            }
            System.out.println();
        }

        return fileShards;
    }
}
