package com.frg96.mapreduce.unittest;

import com.frg96.mapreduce.*;
import com.frg96.mapreduce.api.*;
import com.frg96.mapreduce.worker.MRFrameworkService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

public class MRFrameworkServiceTest {
    @Test
    @SuppressWarnings("unchecked")
    void testMapperSuccessCallsOnNextAndOnCompleted(@TempDir Path tempDir) throws Exception {
        String appId = registerTestTasks();

        Path outputDir = createOutputDirs(tempDir);
        Path inputFile = tempDir.resolve("input.txt");

        Files.writeString(inputFile, """
                apple
                banana
                apple
                """);

        MapperInput request = MapperInput.newBuilder()
                .setAppId(appId)
                .setNumPartitions(2)
                .setOutputDir(outputDir.toString())
                .addInputSplits(ShardSplit.newBuilder()
                        .setFileName(inputFile.toString())
                        .setStartPos(0)
                        .setEndPos(Files.size(inputFile))
                        .build())
                .build();

        MRFrameworkService service = new MRFrameworkService("localhost:50051");
        StreamObserver<MapperOutput> observer = mock(StreamObserver.class);

        service.mapper(request, observer);

        ArgumentCaptor<MapperOutput> captor = ArgumentCaptor.forClass(MapperOutput.class);
        verify(observer, times(1)).onNext(captor.capture());
        verify(observer, times(1)).onCompleted();
        verify(observer, never()).onError(any());

        MapperOutput output = captor.getValue();

        assertEquals(2, output.getFilesCount());

        List<String> allLines = output.getFilesList().stream()
                .map(File::getFileName)
                .map(Path::of)
                .filter(Files::exists)
                .flatMap(path -> {
                    try {
                        return Files.readAllLines(path).stream();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .toList();

        assertTrue(allLines.contains("apple 1"));
        assertTrue(allLines.contains("banana 1"));

    }

    @Test
    @SuppressWarnings("unchecked")
    void testReducerSuccessCallsOnNextAndOnCompleted(@TempDir Path tempDir) throws Exception {
        String appId = registerTestTasks();

        Path outputDir = createOutputDirs(tempDir);

        Path mapperOutput1 = tempDir.resolve("mapper-output-1");
        Path mapperOutput2 = tempDir.resolve("mapper-output-2");

        Files.writeString(mapperOutput1, """
                apple 1
                banana 1
                """);

        Files.writeString(mapperOutput2, """
                apple 1
                carrot 1
                """);

        ReducerInput request = ReducerInput.newBuilder()
                .setAppId(appId)
                .setPartitionId(0)
                .setOutputDir(outputDir.toString())
                .addFiles(File.newBuilder().setFileName(mapperOutput1.toString()).build())
                .addFiles(File.newBuilder().setFileName(mapperOutput2.toString()).build())
                .build();

        MRFrameworkService service = new MRFrameworkService("localhost:50051");
        StreamObserver<ReducerOutput> observer = mock(StreamObserver.class);

        service.reducer(request, observer);

        ArgumentCaptor<ReducerOutput> captor = ArgumentCaptor.forClass(ReducerOutput.class);
        verify(observer).onNext(captor.capture());
        verify(observer).onCompleted();
        verify(observer, never()).onError(any());

        Path reducerOutputFile = Path.of(captor.getValue().getFile().getFileName());

        assertTrue(Files.exists(reducerOutputFile));
        assertEquals(List.of(
                "apple 2",
                "banana 1",
                "carrot 1"
        ), Files.readAllLines(reducerOutputFile));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMapperUnknownAppIdCallsOnError(@TempDir Path tempDir) throws Exception {
        Path outputDir = createOutputDirs(tempDir);
        Path inputFile = tempDir.resolve("input.txt");
        Files.writeString(inputFile, "apple\n");

        MapperInput request = MapperInput.newBuilder()
                .setAppId("missing-app-id")
                .setNumPartitions(2)
                .setOutputDir(outputDir.toString())
                .addInputSplits(ShardSplit.newBuilder()
                        .setFileName(inputFile.toString())
                        .setStartPos(0)
                        .setEndPos(Files.size(inputFile))
                        .build())
                .build();

        MRFrameworkService service = new MRFrameworkService("localhost:50051");
        StreamObserver<MapperOutput> observer = mock(StreamObserver.class);

        service.mapper(request, observer);

        ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);

        verify(observer).onError(captor.capture());
        verify(observer, never()).onNext(any());
        verify(observer, never()).onCompleted();

        assertEquals(Status.Code.INTERNAL, Status.fromThrowable(captor.getValue()).getCode());
    }


    private static Path createOutputDirs(Path tempDir) throws Exception {
        Path outputDir = tempDir.resolve("output");

        Files.createDirectories(outputDir.resolve("intermediate/mapper"));
        Files.createDirectories(outputDir.resolve("intermediate/reducer"));

        return outputDir;
    }

    private static String registerTestTasks() {
        String appId = "mr-service-test-" + UUID.randomUUID();

        boolean registered = TaskRegistry.register(appId, new TaskFactory() {
            @Override
            public Mapper createMapper() {
                return new TestMapper();
            }

            @Override
            public Reducer createReducer() {
                return new TestReducer();
            }
        });

        assertTrue(registered);

        return appId;
    }

    private static final class TestMapper implements Mapper {
        @Override
        public void map(String inputLine, MapperContext context) {
            context.emit(inputLine, "1");
        }
    }

    private static final class TestReducer implements Reducer {
        @Override
        public void reduce(String key, List<String> values, ReducerContext context) {
            int sum = values.stream()
                    .mapToInt(Integer::parseInt)
                    .sum();

            context.emit(key, Integer.toString(sum));
        }
    }
}
