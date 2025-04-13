package org.grid.distributor;

import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.grid.GridComms;
import org.grid.GridManagerGrpc;
import org.grid.ResultReceiverGrpc;
import org.grid.SubtaskExchangeGrpc;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Distributor {
    private final static Logger logger = Logger.getLogger(Distributor.class.getName());

    private static final int CHUNK_SIZE = 8 * 1024;
    private static final int SUBTASK_TIMEOUT_MINUTES = 20;
    private static final int RESULT_SERVER_PORT = 10000;
    private static final String DISTRIBUTOR_HOST = "localhost";
    private static final int MAX_RETRIES = 3;

    private final String managerHost;
    private final int managerPort;
    private ManagedChannel managerChannel;
    private GridManagerGrpc.GridManagerBlockingStub managerStub;

    private Server resultServer;
    private final int resultPort;
    private final String distributorHost;

    private final String taskDirectory;
    private final Task task;

    private volatile boolean skipCurrentFileUpload = false;
    private String currentlySendingFile = null;

    private final ScheduledExecutorService subtaskMonitoringScheduler = Executors.newSingleThreadScheduledExecutor();

    public Distributor(String managerHost, int managerPort, String taskDirectory, long taskId) throws InterruptedException {
        this(managerHost, managerPort, taskDirectory, DISTRIBUTOR_HOST, RESULT_SERVER_PORT, taskId);
    }

    public Distributor(String managerHost, int managerPort, String taskDirectory, String distributorHost, int resultPort, long taskId) throws InterruptedException {
        this.managerHost = managerHost;
        this.managerPort = managerPort;
        this.taskDirectory = taskDirectory;
        this.distributorHost = distributorHost;
        this.resultPort = resultPort;
        this.task = new Task(taskId, taskDirectory);
        openManagerChannel();
        subtaskMonitoringScheduler.scheduleWithFixedDelay(() -> {
            try {
                checkRunningTasks();
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Ошибка при выполнении проверки задач: " + e.getMessage(), e);
            }
        }, SUBTASK_TIMEOUT_MINUTES, SUBTASK_TIMEOUT_MINUTES, TimeUnit.SECONDS);
    }

    private synchronized void openManagerChannel() {
        if (managerChannel == null || managerChannel.isShutdown()) {
            managerChannel = ManagedChannelBuilder.forAddress(managerHost, managerPort)
                    .usePlaintext()
                    .build();
            managerStub = GridManagerGrpc.newBlockingStub(managerChannel);
            logger.info("Opened manager channel to " + managerHost + ":" + managerPort);
        }
    }

    public synchronized void shutdownManagerChannel() {
        if (managerChannel != null) {
            managerChannel.shutdown();
            try {
                if (!managerChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                    managerChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                managerChannel.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Manager channel shutdown.");
        }
    }

    public void shutdownScheduler() {
        logger.info("Shutting down scheduled executor service...");
        subtaskMonitoringScheduler.shutdown();
        try {
            if (!subtaskMonitoringScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                logger.info("Scheduled executor did not terminate in the given time. Forcing shutdown.");
                subtaskMonitoringScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            subtaskMonitoringScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void startResultServer() throws IOException {
        if (resultServer == null || resultServer.isTerminated()) {
            logger.info("Starting Result Receiver server on port " + resultPort);
            resultServer = ServerBuilder.forPort(resultPort)
                    .addService(new ResultReceiverImpl(task))
                    .build()
                    .start();
            logger.info("Result Receiver server started on port " + resultPort);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down Result Receiver server due to JVM shutdown.");
                stopResultServer();
            }));
        } else {
            logger.info("Result Receiver server is already running.");
        }
    }

    public void blockUntilResultServerShutdown() throws InterruptedException {
        if (resultServer != null) {
            resultServer.awaitTermination();
        }
    }

    public void stopResultServer() {
        if (resultServer != null && !resultServer.isShutdown()) {
            logger.info("Stopping Result Receiver server...");
            resultServer.shutdown();
            try {
                if (!resultServer.awaitTermination(10, TimeUnit.SECONDS)) {
                    logger.warning("Result Receiver server did not terminate gracefully. Forcing shutdown.");
                    resultServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                logger.log(Level.SEVERE, "Interrupted while waiting for Result Receiver server shutdown: " + e.getMessage(), e);
                resultServer.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public Task getTask() {
        return task;
    }

    public void runTask() {
        long taskId = task.getId();
        Iterator<?> subtaskIterator = task.getSubtaskIterator();
        long subtaskId = 0;
        while (subtaskIterator.hasNext()) {
            Object nextSubtask = subtaskIterator.next();
            try {
                ByteString inputDataBytes = serializeToByteString(nextSubtask);
                subtaskId++;
                submitSubtask(taskId, subtaskId, inputDataBytes);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to serialize subtask " + subtaskId + ": " + e.getMessage(), e);
                throw new RuntimeException(e);
            }
        }
    }

    public void submitSubtask(long taskId, long subtaskId, ByteString byteString) {
        int attempt = 0;
        boolean success = false;
        try {
            task.addResult(new Subtask(subtaskId, SubtaskStatus.RUNNING, null, System.currentTimeMillis(), byteString));
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        while (attempt < MAX_RETRIES && !success) {
            logger.info(String.format("Submitting subtask %d/%d, attempt %d", taskId, subtaskId, attempt + 1));
            success = attemptSubmitSubtask(taskId, subtaskId, byteString);
            if (!success) {
                logger.warning(String.format("Attempt %d for subtask %d/%d failed", taskId, subtaskId, attempt + 1));
                attempt++;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (!success) {
            logger.severe(String.format("Subtask %d/%d failed after %d attempts. Marking as FAILED.", taskId, subtaskId, MAX_RETRIES));
            try {
                task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), byteString));
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean attemptSubmitSubtask(long taskId, long subtaskId, ByteString byteString) {
        String[] files = task.getFileNames();
        logger.info(String.format("Trying to submit subtask %d/%d with files: %s", taskId, subtaskId, Arrays.toString(files)));

        GridComms.WorkerAssignment workerAssignment = requestWorkerFromManager(taskId, subtaskId);
        if (!workerAssignment.getWorkerAvailable()) {
            logger.info(String.format("No available worker for task %d/%d: %s", taskId, subtaskId, workerAssignment.getMessage()));
            return false;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(workerAssignment.getAssignedWorker().getHost(), workerAssignment.getAssignedWorker().getPort())
                    .usePlaintext()
                    .build();

            SubtaskExchangeGrpc.SubtaskExchangeStub stub = SubtaskExchangeGrpc.newStub(channel);
            CountDownLatch latch = new CountDownLatch(1);
            final boolean[] overallSuccess = {true};

            StreamObserver<GridComms.SubtaskUploadResponse> responseObserver = new StreamObserver<GridComms.SubtaskUploadResponse>() {
                @Override
                public void onNext(GridComms.SubtaskUploadResponse response) {
                    if (response.hasAck()) {
                        logger.info(String.format("Ack received for subtask %d/%d: %s", taskId, subtaskId, response.getAck().getMessage()));
                    } else if (response.hasFileStatus()) {
                        GridComms.FileStatus status = response.getFileStatus();
                        logger.info(String.format("File status for subtask %d/%d: file=%s, success=%s, detail=%s, bytes received=%d",
                                taskId, subtaskId, status.getFileName(), status.getSuccess(), status.getDetail(), status.getBytesReceived()));
                        skipCurrentFileUpload = false;
                        if (currentlySendingFile != null &&
                                currentlySendingFile.equals(status.getFileName()) &&
                                String.format("File %s already exists%n", currentlySendingFile).equals(status.getDetail())) {
                            logger.info(String.format("Worker indicates file %s already exists, aborting upload for this file", status.getFileName()));
                            skipCurrentFileUpload = true;
                        }
                    } else if (response.hasFinalResult()) {
                        GridComms.OverallResult result = response.getFinalResult();
                        logger.info(String.format("Final result for subtask %d/%d: Success=%s, Processed=%d, Message=%s",
                                taskId, subtaskId, result.getOverallSuccess(), result.getFilesProcessedCount(), result.getFinalMessage()));
                    } else if (response.hasError()) {
                        GridComms.ErrorDetails errorDetails = response.getError();
                        logger.severe(String.format("Error for subtask %d/%d: %s, related file: %s",
                                taskId, subtaskId, errorDetails.getErrorMessage(), errorDetails.getFailedFileName()));
                        if (errorDetails.getFatal()) {
                            overallSuccess[0] = false;
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.SEVERE, String.format("Error uploading subtask %d/%d: %s", taskId, subtaskId, Status.fromThrowable(t)), t);
                    overallSuccess[0] = false;
                    latch.countDown();
                }

                @Override
                public void onCompleted() {
                    logger.info(String.format("Subtask %d/%d upload completed", taskId, subtaskId));
                    latch.countDown();
                }
            };

            StreamObserver<GridComms.SubtaskUploadMessage> uploadObserver = stub.sendSubtask(responseObserver);
            logger.info(String.format("Sending metadata for subtask %d/%d", taskId, subtaskId));
            GridComms.SubtaskMetadata metadata = GridComms.SubtaskMetadata.newBuilder()
                    .setTaskId(taskId)
                    .setSubtaskId(subtaskId)
                    .setDistributorHost(distributorHost)
                    .setDistributorPort(resultPort)
                    .setSubtaskInputData(byteString)
                    .build();
            uploadObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setSubtaskMetadata(metadata).build());

            A:
            for (String fileName : files) {
                Path path = Paths.get(taskDirectory, fileName);
                if (!Files.exists(path) || !Files.isReadable(path)) {
                    throw new RuntimeException("Cannot read file: " + path);
                }
                currentlySendingFile = fileName;
                skipCurrentFileUpload = false;
                logger.info(String.format("Sending header for file %s", path.getFileName()));
                GridComms.FileHeader header = GridComms.FileHeader.newBuilder()
                        .setFileName(path.getFileName().toString())
                        .build();
                uploadObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setFileHeader(header).build());

                long bytesSent = 0;
                try (InputStream in = Files.newInputStream(path)) {
                    byte[] buffer = new byte[CHUNK_SIZE];
                    int read;
                    while ((read = in.read(buffer)) != -1) {
                        if (skipCurrentFileUpload) {
                            logger.info("Upload skipped for file " + fileName);
                            break A;
                        }
                        GridComms.FileChunk chunk = GridComms.FileChunk.newBuilder().setData(ByteString.copyFrom(buffer, 0, read)).build();
                        uploadObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setFileChunk(chunk).build());
                        bytesSent += read;
                    }
                    logger.info(String.format("Sent %d bytes for file %s", bytesSent, path.getFileName()));
                } catch (IOException e) {
                    uploadObserver.onError(Status.INTERNAL.withDescription("Failed to read file: " + path.getFileName())
                            .withCause(e).asRuntimeException());
                    return false;
                } finally {
                    currentlySendingFile = null;
                }
            }

            logger.info(String.format("All data for subtask %d/%d sent", taskId, subtaskId));
            uploadObserver.onCompleted();
            try {
                if (!latch.await(SUBTASK_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                    logger.severe(String.format("Worker did not respond in time for subtask %d/%d", taskId, subtaskId));
                    overallSuccess[0] = false;
                    responseObserver.onError(Status.CANCELLED.withDescription("Timeout waiting for worker reply").asRuntimeException());
                }
            } catch (InterruptedException e) {
                overallSuccess[0] = false;
                Thread.currentThread().interrupt();
            }
            logger.info(String.format("Submission ended for subtask %d/%d, success=%s", taskId, subtaskId, overallSuccess[0]));
            return overallSuccess[0];
        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("Exception in submission for subtask %d/%d: %s", taskId, subtaskId, e.getMessage()), e);
            return false;
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    private GridComms.WorkerAssignment requestWorkerFromManager(long taskId, long subtaskId) {
        try {
            GridComms.SubtaskRequest request = GridComms.SubtaskRequest.newBuilder()
                    .setTaskId(taskId)
                    .setSubtaskId(subtaskId)
                    .build();
            logger.info(String.format("Requesting worker for task %d/%d", taskId, subtaskId));
            return managerStub.withDeadlineAfter(15, TimeUnit.SECONDS).requestWorker(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.SEVERE, "Error requesting worker: " + e.getStatus(), e);
            return GridComms.WorkerAssignment.newBuilder()
                    .setWorkerAvailable(false)
                    .setMessage("Error contacting manager: " + e.getStatus())
                    .build();
        }
    }

    private void checkRunningTasks() {
        logger.info("Start checking running tasks");
        var runningTasks = task.getResults().entrySet().stream()
                .filter(entry -> entry.getValue().getStatus().equals(SubtaskStatus.RUNNING))
                .toList();
        for (Map.Entry<Long, Subtask> entry : runningTasks) {
            long subtaskId = entry.getValue().getId();
            GridComms.WorkerStatusRequest request = GridComms.WorkerStatusRequest.newBuilder()
                    .setTaskId(task.getId())
                    .setSubtaskId(subtaskId)
                    .build();
            try {
                GridComms.WorkerStatusResponse response = managerStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .getWorkerStatus(request);
                if (response.getWorkerStatus().equals(GridComms.WorkerStatus.DEAD)
                        || response.getWorkerStatus().equals(GridComms.WorkerStatus.UNKNOWN)) {
                    Subtask failedTask = task.getResults().get(subtaskId);
                    task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), failedTask.getData()));
                }
            } catch (StatusRuntimeException e) {
                logger.log(Level.SEVERE, String.format("Error checking status for subtask %d: %s", subtaskId, e.getStatus()), e);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        logger.info("Finished checking running tasks");
        reassignFailedSubtasks();
    }

    private void reassignFailedSubtasks() {
        logger.info(task.getResults().toString());
        for (Map.Entry<Long, Subtask> entry : task.getResults().entrySet()) {
            if (entry.getValue().getStatus().equals(SubtaskStatus.FAILED)) {
                long subtaskId = entry.getKey();
                ByteString inputData = entry.getValue().getData();
                if (inputData == null) {
                    throw new RuntimeException("Cannot reassign subtask " + entry.getValue().getId());
                }
                logger.info(String.format("Reassigning failed subtask %d", subtaskId));
                submitSubtask(task.getId(), subtaskId, inputData);
            }
        }
    }

    private static ByteString serializeToByteString(Object obj) throws Exception {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            oos.flush();
            return ByteString.copyFrom(baos.toByteArray());
        }
    }

    private static class ResultReceiverImpl extends ResultReceiverGrpc.ResultReceiverImplBase {
        private final Task task;

        public ResultReceiverImpl(Task task) {
            this.task = task;
        }

        @Override
        public void submitResult(GridComms.SubtaskResult request, StreamObserver<GridComms.ResultAcknowledgement> responseObserver) {
            long taskId = request.getTaskId();
            long subtaskId = request.getSubtaskId();
            String workerId = request.getWorkerId();
            logger.info(String.format("Received result for task %d/%d from worker %s", taskId, subtaskId, workerId));

            GridComms.ResultAcknowledgement.Builder ackBuilder = GridComms.ResultAcknowledgement.newBuilder();

            if (task == null) {
                logger.warning("Received result for unknown or completed task " + taskId);
                ackBuilder.setAcknowledged(false)
                        .setMessage("Task " + taskId + " not found or already completed.");
            } else {
                ByteString resultBytes = request.getResultData();
                logger.info("Result object: " + resultBytes);
                try {
                    if (!request.getSuccess()) {
                        logger.warning(String.format("Result for subtask %d/%d indicates failure. Scheduling reassignment.", taskId, subtaskId));
                        task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, resultBytes, System.currentTimeMillis()));
                    } else {
                        task.addResult(new Subtask(subtaskId, SubtaskStatus.DONE, resultBytes, System.currentTimeMillis()));
                    }
                } catch (ClassNotFoundException | IOException e) {
                    throw new RuntimeException(e);
                }
                ackBuilder.setAcknowledged(true).setMessage("Result processed successfully.");
            }
            responseObserver.onNext(ackBuilder.build());
            responseObserver.onCompleted();
        }
    }
}

class TaskSpecificObjectInputStream extends ObjectInputStream {
    private final ClassLoader taskClassLoader;

    public TaskSpecificObjectInputStream(InputStream in, ClassLoader taskClassLoader) throws IOException {
        super(in);
        this.taskClassLoader = Objects.requireNonNull(taskClassLoader, "Task ClassLoader cannot be null");
    }

    @Override
    protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
        String className = desc.getName();
        try {
            return Class.forName(className, false, taskClassLoader);
        } catch (ClassNotFoundException e) {
            return super.resolveClass(desc);
        }
    }
}
