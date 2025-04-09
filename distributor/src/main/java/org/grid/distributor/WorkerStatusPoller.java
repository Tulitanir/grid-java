package org.grid.distributor;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.grid.GridComms;
import org.grid.GridManagerGrpc;

import java.util.concurrent.TimeUnit;

public class WorkerStatusPoller {
    private final String managerHost;
    private final int managerPort;
    private ManagedChannel pollChannel;
    private GridManagerGrpc.GridManagerBlockingStub pollStub;

    public WorkerStatusPoller(String managerHost, int managerPort) {
        this.managerHost = managerHost;
        this.managerPort = managerPort;
        openChannel();
    }

    private synchronized void openChannel() {
        if (pollChannel == null || pollChannel.isShutdown()) {
            pollChannel = ManagedChannelBuilder.forAddress(managerHost, managerPort)
                    .usePlaintext()
                    .build();
            pollStub = GridManagerGrpc.newBlockingStub(pollChannel);
        }
    }

    public GridComms.WorkerStatusResponse getWorkerStatus(GridComms.WorkerStatusRequest request) {
        try {
            return pollStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                    .getWorkerStatus(request);
        } catch (StatusRuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void shutdown() {
        if (pollChannel != null) {
            pollChannel.shutdown();
            try {
                if (!pollChannel.awaitTermination(10, TimeUnit.SECONDS)) {
                    pollChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                pollChannel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
