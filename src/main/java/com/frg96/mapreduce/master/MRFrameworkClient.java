package com.frg96.mapreduce.master;

import com.frg96.mapreduce.*;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Blocking gRPC client used by the master to execute tasks on a worker.
 *
 * <p>Each client owns a channel to one worker and applies a deadline to
 * mapper and reducer requests. The channel must be released with
 * {@link #shutdown()} when the job finishes.</p>
 *
 * <p>The current implementation uses insecure channel credentials and is
 * intended for trusted environments.</p>
 */
class MRFrameworkClient {
    private static final Logger LOGGER = Logger.getLogger(MRFrameworkClient.class.getName());

    private static final int MAPPER_TIMEOUT_SECONDS = 10;
    private static final int REDUCER_TIMEOUT_SECONDS = 10;

    private final ManagedChannel channel;
    private final MRFrameworkGrpc.MRFrameworkBlockingStub stub;

    /**
     * Creates a client connected to the given worker.
     *
     * @param workerAddress worker address in {@code host:port} format
     */
    MRFrameworkClient(String workerAddress) {
        this.channel = Grpc.newChannelBuilder(workerAddress, InsecureChannelCredentials.create())
                .build();

        this.stub = MRFrameworkGrpc.newBlockingStub(channel);

        LOGGER.log(Level.INFO, "Created worker channel for {0}", workerAddress);
    }

    /**
     * Executes a mapper task using the configured worker.
     *
     * <p>This is a blocking call with a deadline of
     * {@value #MAPPER_TIMEOUT_SECONDS} seconds.</p>
     *
     * @param input mapper task request
     * @return mapper result containing intermediate partition files
     * @throws StatusRuntimeException if the RPC fails or exceeds its deadline
     */
    MapperOutput callMapper(MapperInput input) throws StatusRuntimeException {
        return stub
                .withDeadlineAfter(MAPPER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .mapper(input);
    }

    /**
     * Executes a reducer task using the configured worker.
     *
     * <p>This is a blocking call with a deadline of
     * {@value #REDUCER_TIMEOUT_SECONDS} seconds.</p>
     *
     * @param input reducer task request
     * @return reducer result containing the generated output file
     * @throws StatusRuntimeException if the RPC fails or exceeds its deadline
     */
    ReducerOutput callReducer(ReducerInput input) throws StatusRuntimeException {
        return stub
                .withDeadlineAfter(REDUCER_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .reducer(input);
    }

    /**
     * Gracefully shuts down the worker channel.
     *
     * <p>If the channel does not terminate within five seconds, it is forcibly
     * closed. If interrupted while waiting, this method restores the thread's
     * interrupted status and logs the interruption.</p>
     */
    void shutdown(){
        try {
            channel.shutdown();
            if(!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                LOGGER.warning("Worker channel did not terminate gracefully; forcing shutdown");
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Interrupted while shutting down worker channel", e);
        }
    }

}
