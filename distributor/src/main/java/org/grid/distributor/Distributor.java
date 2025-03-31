package org.grid.distributor;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.grid.GridComms;
import org.grid.SubtaskExchangeGrpc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Distributor {
    private static final int CHUNK_SIZE = 8 * 1024;
    private static final int TIMEOUT = 10;
    private ManagedChannel channel;
    private SubtaskExchangeGrpc.SubtaskExchangeStub stub;
    private final String host;
    private final int port;
    private final String workingDir;
    private final Task currentTask;
    private Server server;
    private List<GridComms.WorkerRegistrationMessage> workers;

    private volatile boolean skipCurrentFileUpload = false;
    private String currentlySendingFile = null;

    public Distributor(String host, int port, String workingDir) {
        this.host = host;
        this.port = port;
        this.workingDir = workingDir;
        currentTask = new Task(workingDir);
    }

    private void connect() {
        if (channel == null || channel.isShutdown() || channel.isTerminated()) {
            channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();

            stub = SubtaskExchangeGrpc.newStub(channel);
            System.out.println("Create new channel");
        } else {
            System.out.println("Reusing existing channel");
        }
    }

    private void disconnect() {
        if (channel != null && !channel.isShutdown()) {
            System.out.println("Shutting down channel");

            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                System.out.println("Channel shutdown interrupted");
                Thread.currentThread().interrupt();
            } finally {
                channel = null;
                stub = null;
            }
        }
    }

    public boolean submitSubtask(long taskId, long subtaskId) {
        String[] files = currentTask.getFileNames();
        System.out.printf("Try to submit subtask %s/%s with files: %s%n", taskId, subtaskId, Arrays.toString(files));
        connect();

        CountDownLatch countDownLatch = new CountDownLatch(1);
        final boolean[] overallSuccess = {true};

        StreamObserver<GridComms.SubtaskUploadResponse> responseStreamObserver = new StreamObserver<GridComms.SubtaskUploadResponse>() {
            @Override
            public void onNext(GridComms.SubtaskUploadResponse subtaskUploadResponse) {
                if (subtaskUploadResponse.hasAck()) {
                    System.out.printf("Got acknowledgement from worker with subtask %s/%s: %s%n", taskId, subtaskId, subtaskUploadResponse.getAck().getMessage());
                } else if (subtaskUploadResponse.hasFileStatus()) {
                    GridComms.FileStatus status = subtaskUploadResponse.getFileStatus();
                    System.out.printf("Worker with subtask %s/%s file status: %s, success: %s, detail: %s, bytes received: %s%n", taskId, subtaskId, status.getFileName(), status.getSuccess(), status.getDetail(), status.getBytesReceived());
                    skipCurrentFileUpload = false;
                    if (currentlySendingFile != null && currentlySendingFile.equals(status.getFileName()) && String.format("File %s already exists%n", currentlySendingFile).equals(status.getDetail())) {
                        System.out.printf("Worker indicated file %s already exists. Aborting upload for this file.%n", status.getFileName());
                        skipCurrentFileUpload = true;
                    }
                } else if (subtaskUploadResponse.hasFinalResult()) {
                    GridComms.OverallResult result = subtaskUploadResponse.getFinalResult();
                    System.out.printf("Worker with subtask %s/%s upload final result: Success=%s, FileProcessed=%s, Message=%s%n", taskId, subtaskId, result.getOverallSuccess(), result.getFilesProcessedCount(), result.getFinalMessage());
                } else if (subtaskUploadResponse.hasError()) {
                    GridComms.ErrorDetails errorDetails = subtaskUploadResponse.getError();
                    System.err.printf("Worker with subtask %s/%s got error: %s, related to file: %s%n", taskId, subtaskId, errorDetails.getErrorMessage(), errorDetails.getFailedFileName());
                    if(errorDetails.getFatal()){
                        overallSuccess[0] = false;
                        System.out.println("Бля");
                    }
                } else {
                    System.out.println("Received unknown response type from server.");
                }
            }

            @Override
            public void onError(Throwable throwable) {
                System.err.printf("Worker with subtask %s/%s got error: %s%n", taskId, subtaskId, Status.fromThrowable(throwable));
            }

            @Override
            public void onCompleted() {
                System.out.printf("Subtask %s/%s upload is successful%n", taskId, subtaskId);
                countDownLatch.countDown();
            }
        };

        boolean clientError = false;
        StreamObserver<GridComms.SubtaskUploadMessage> uploadMessageStreamObserver = stub.sendSubtask(responseStreamObserver);
        try {
            System.out.printf("Sending subtask %s/%s metadata%n", taskId, subtaskId);
            GridComms.SubtaskMetadata metadata = GridComms.SubtaskMetadata.newBuilder()
                    .setTaskId(taskId)
                    .setSubtaskId(subtaskId)
                    .build();

            uploadMessageStreamObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setSubtaskMetadata(metadata).build());

            A: for (String fileName : files) {
                Path path = Paths.get(workingDir, fileName);

                if (!Files.exists(path) || !Files.isReadable(path)) {
                    throw new RuntimeException("Can't find or read file: " + path);
                }

                currentlySendingFile = fileName;
                skipCurrentFileUpload = false;

                System.out.printf("Sending file header for %s%n", path.getFileName());

                GridComms.FileHeader header = GridComms.FileHeader.newBuilder()
                        .setFileName(path.getFileName().toString())
                        .build();

                uploadMessageStreamObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setFileHeader(header).build());

                long bytesSentForFile = 0;
                try (InputStream inputStream = Files.newInputStream(path)){
                    byte[] buffer = new byte[CHUNK_SIZE];
                    int bytesRead;
                    while (true) {
                        if (skipCurrentFileUpload) {
                            System.out.printf("Upload skipped for file %s as requested by worker.%n", fileName);
                            break A;
                        }
                        bytesRead = inputStream.read(buffer);
                        if (bytesRead == -1)
                            break;
                        GridComms.FileChunk fileChunk = GridComms.FileChunk.newBuilder()
                                .setData(ByteString.copyFrom(buffer, 0, bytesRead))
                                .build();
                        uploadMessageStreamObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setFileChunk(fileChunk).build());
                        bytesSentForFile += bytesRead;
                    }
                    System.out.printf("Sent %s bytes of file %s%n", bytesSentForFile, path.getFileName());
                } catch (IOException e) {
                    uploadMessageStreamObserver.onError(Status.INTERNAL.withDescription("Failed to read file: " + path.getFileName()).withCause(e).asRuntimeException());
                    clientError = true;
                    break;
                } finally {
                    currentlySendingFile = null;
                }
            }

            if (!clientError) {
                System.out.printf("Subtask %s/%s was successfully sent%n", taskId, subtaskId);
                uploadMessageStreamObserver.onCompleted();
            }

            System.out.println("Waiting for worker reply");
            if (!countDownLatch.await(TIMEOUT, TimeUnit.MINUTES)) {
                System.err.printf("Worker didn't reply in %s minutes for subtask %s/%s%n", TIMEOUT, taskId, subtaskId);
                responseStreamObserver.onError(Status.CANCELLED.withDescription("Distributor timeout waiting for worker completion").asRuntimeException());
                overallSuccess[0] = false;
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            disconnect();
            System.out.printf("Task submission ended. ID: %s/%s, success: %s%n", taskId, subtaskId, overallSuccess[0]);
        }

        return overallSuccess[0];
    }
}
