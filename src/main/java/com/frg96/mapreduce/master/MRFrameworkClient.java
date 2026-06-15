package com.frg96.mapreduce.master;

import com.frg96.mapreduce.*;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

class MRFrameworkClient {
    private static final Logger logger = Logger.getLogger(MRFrameworkClient.class.getName());

    private static final int MAPPER_TIMEOUT_SECONDS = 10;
    private static final int REDUCER_TIMEOUT_SECONDS = 10;

    private final ManagedChannel channel;
    private final MRFrameworkGrpc.MRFrameworkBlockingStub stub;

    MRFrameworkClient(String workerAddress) {
        this.channel = Grpc.newChannelBuilder(workerAddress, InsecureChannelCredentials.create())
                .build();

        this.stub = MRFrameworkGrpc.newBlockingStub(channel);
    }

    MapperOutput callMapper(MapperInput input) throws StatusRuntimeException {
        return stub
                .withDeadlineAfter(MAPPER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .mapper(input);
    }

    ReducerOutput callReducer(ReducerInput input) throws StatusRuntimeException {
        return stub
                .withDeadlineAfter(REDUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .reducer(input);
    }

    void shutdown(){
        try {
            channel.shutdown();
            if(!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.severe("MRFrameworkClient shutdown interrupted");
        }
    }

}
