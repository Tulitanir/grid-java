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

    private ManagedChannel openManagerChannel() {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(managerHost, managerPort)
                .usePlaintext()
                .build();
        logger.info("Opened temporary manager channel to " + managerHost + ":" + managerPort);
        return channel;
    }

    public void shutdownManagerChannel(ManagedChannel channel) {
        if (channel != null && !channel.isShutdown()) {
            logger.info("Shutting down temporary manager channel...");
            channel.shutdown();
            try {
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
                logger.info("Temporary manager channel shutdown complete.");
            } catch (InterruptedException e) {
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
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
        long subtaskIdCounter = 0; // Используем счетчик для ID

        while (subtaskIterator.hasNext()) {
            Object nextSubtask = subtaskIterator.next();
            subtaskIdCounter++; // Увеличиваем ID для каждой новой подзадачи
            final long currentSubtaskId = subtaskIdCounter; // final для использования в лямбдах/потоках

            try {
                ByteString inputDataBytes = serializeToByteString(nextSubtask);
                logger.info("Prepared subtask " + taskId + "/" + currentSubtaskId);

                try {
                    task.addResult(new Subtask(currentSubtaskId, SubtaskStatus.PENDING, null, System.currentTimeMillis(), inputDataBytes));
                } catch (IOException | ClassNotFoundException e) {
                    logger.log(Level.SEVERE, "Failed to add initial PENDING status for subtask " + taskId + "/" + currentSubtaskId, e);
                     continue;
                }

                submitSubtaskWithRetries(taskId, currentSubtaskId, inputDataBytes);

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to serialize subtask " + taskId + "/" + currentSubtaskId + ": " + e.getMessage(), e);
                try {
                    Subtask existing = task.getResults().get(currentSubtaskId);
                    ByteString data = (existing != null) ? existing.getData() : null;
                    task.addResult(new Subtask(currentSubtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), data));
                } catch (Exception innerEx) {
                    logger.log(Level.SEVERE, "Failed to mark subtask " + taskId + "/" + currentSubtaskId + " as FAILED after serialization error", innerEx);
                }
            }
        }

        logger.info("All subtasks from iterator have been processed for task " + taskId);
    }

    public void submitSubtaskWithRetries(long taskId, long subtaskId, ByteString byteString) {
        int attempt = 0;
        boolean success = false;

        while (attempt < MAX_RETRIES && !success) {
            logger.info(String.format("Attempting to dispatch subtask %d/%d (Attempt %d/%d)", taskId, subtaskId, attempt + 1, MAX_RETRIES));
            success = attemptDispatchSubtask(taskId, subtaskId, byteString);

            if (!success) {
                logger.warning(String.format("Dispatch attempt %d for subtask %d/%d failed.", attempt + 1, taskId, subtaskId));
                attempt++;
                if (attempt < MAX_RETRIES) {
                    try {
                        Thread.sleep(2000L * (attempt));
                    } catch (InterruptedException e) {
                        logger.warning("Retry delay interrupted for subtask " + taskId + "/" + subtaskId);
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        if (!success) {
            logger.severe(String.format("Subtask %d/%d failed to dispatch after %d attempts. Marking as FAILED.", taskId, subtaskId, MAX_RETRIES));
            try {
                task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), byteString));
            } catch (IOException | ClassNotFoundException e) {
                logger.log(Level.SEVERE, "Failed to mark subtask " + taskId + "/" + subtaskId + " as FAILED locally after dispatch failures", e);
            }
        } else {
            logger.info(String.format("Subtask %d/%d dispatched successfully.", taskId, subtaskId));
        }
    }

    private boolean attemptDispatchSubtask(long taskId, long subtaskId, ByteString byteString) {
        logger.info(String.format("Attempting dispatch for subtask %d/%d: Requesting worker...", taskId, subtaskId));

        GridComms.WorkerAssignment workerAssignment = requestWorkerFromManager(taskId, subtaskId);

        if (!workerAssignment.getWorkerAvailable()) {
            logger.warning(String.format("No available worker from manager for task %d/%d: %s", taskId, subtaskId, workerAssignment.getMessage()));
            return false;
        }

        logger.info(String.format("Worker %s assigned for subtask %d/%d. Proceeding to send data.",
                workerAssignment.getAssignedWorker().getWorkerId(), taskId, subtaskId));

        try {
            task.addResult(new Subtask(subtaskId, SubtaskStatus.RUNNING, null, System.currentTimeMillis(), byteString));
            logger.info("Marked subtask " + taskId + "/" + subtaskId + " as RUNNING locally.");
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Failed to mark subtask " + taskId + "/" + subtaskId + " as RUNNING before sending data", e);
            return false;
        }

        return sendDataToWorker(workerAssignment, taskId, subtaskId, byteString);
    }

    private boolean sendDataToWorker(GridComms.WorkerAssignment workerAssignment, long taskId, long subtaskId, ByteString byteString) {
        String workerHost = workerAssignment.getAssignedWorker().getHost();
        int workerPort = workerAssignment.getAssignedWorker().getPort();
        String workerId = workerAssignment.getAssignedWorker().getWorkerId();

        logger.info(String.format("Starting data transmission for subtask %d/%d to worker %s at %s:%d",
                taskId, subtaskId, workerId, workerHost, workerPort));

        String[] files = task.getFileNames();
        ManagedChannel workerChannel = null;
        final boolean[] overallSuccess = {true};

        try {
            workerChannel = ManagedChannelBuilder
                    .forAddress(workerHost, workerPort)
                    .usePlaintext()
                    .build();

            SubtaskExchangeGrpc.SubtaskExchangeStub stub = SubtaskExchangeGrpc.newStub(workerChannel);
            CountDownLatch latch = new CountDownLatch(1);

            StreamObserver<GridComms.SubtaskUploadResponse> responseObserver = new StreamObserver<GridComms.SubtaskUploadResponse>() {
                @Override
                public void onNext(GridComms.SubtaskUploadResponse response) {
                    if (response.hasAck()) {
                        logger.info(String.format("Ack received for subtask %d/%d from worker %s: %s", taskId, subtaskId, workerId, response.getAck().getMessage()));
                    } else if (response.hasFileStatus()) {
                        GridComms.FileStatus status = response.getFileStatus();
                        logger.info(String.format("File status for subtask %d/%d from worker %s: file=%s, success=%s, detail=%s, bytes received=%d",
                                taskId, subtaskId, workerId, status.getFileName(), status.getSuccess(), status.getDetail(), status.getBytesReceived()));
                        skipCurrentFileUpload = false;
                        if (currentlySendingFile != null &&
                                currentlySendingFile.equals(status.getFileName())) {
                            status.getDetail();
                            if (status.getDetail().contains("already exists")) {
                                logger.info(String.format("Worker %s indicates file %s already exists, skipping upload for this file.", workerId, status.getFileName()));
                                skipCurrentFileUpload = true;
                            }
                        }
                        if (!status.getSuccess()) {
                            logger.warning("File upload failed according to worker status: " + status.getFileName());
                             overallSuccess[0] = false;
                        }
                    } else if (response.hasFinalResult()) {
                        GridComms.OverallResult result = response.getFinalResult();
                        logger.info(String.format("Final result for subtask %d/%d from worker %s: Success=%s, Processed=%d, Message=%s",
                                taskId, subtaskId, workerId, result.getOverallSuccess(), result.getFilesProcessedCount(), result.getFinalMessage()));
                        if (!result.getOverallSuccess()) {
                            logger.warning("Worker reported overall failure in final result.");
                            overallSuccess[0] = false;
                        }
                    } else if (response.hasError()) {
                        GridComms.ErrorDetails errorDetails = response.getError();
                        logger.severe(String.format("Error reported by worker %s for subtask %d/%d: %s, related file: %s, fatal: %s",
                                workerId, taskId, subtaskId, errorDetails.getErrorMessage(), errorDetails.getFailedFileName(), errorDetails.getFatal()));
                        if (errorDetails.getFatal()) {
                            overallSuccess[0] = false;
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.log(Level.SEVERE, String.format("Error during gRPC stream with worker %s for subtask %d/%d: %s", workerId, taskId, subtaskId, Status.fromThrowable(t)), t);
                    overallSuccess[0] = false;
                    if (latch.getCount() > 0) latch.countDown();
                }

                @Override
                public void onCompleted() {
                    logger.info(String.format("Worker %s completed response stream for subtask %d/%d.", workerId, taskId, subtaskId));
                    if (latch.getCount() > 0) latch.countDown();
                }
            };

            StreamObserver<GridComms.SubtaskUploadMessage> uploadObserver = stub.sendSubtask(responseObserver);

            logger.info(String.format("Sending metadata for subtask %d/%d to worker %s", taskId, subtaskId, workerId));
            GridComms.SubtaskMetadata metadata = GridComms.SubtaskMetadata.newBuilder()
                    .setTaskId(taskId)
                    .setSubtaskId(subtaskId)
                    .setDistributorHost(distributorHost)
                    .setDistributorPort(resultPort)
                    .setSubtaskInputData(byteString)
                    .build();
            uploadObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setSubtaskMetadata(metadata).build());

            for (String fileName : files) {
                if (!overallSuccess[0]) {
                    logger.warning("Skipping remaining file uploads for subtask " + taskId + "/" + subtaskId + " due to previous fatal error.");
                    break;
                }

                Path path = Paths.get(taskDirectory, fileName);
                if (!Files.exists(path) || !Files.isReadable(path)) {
                    logger.severe("Cannot read file: " + path + ". Failing subtask.");
                    uploadObserver.onError(Status.FAILED_PRECONDITION.withDescription("Distributor cannot read file: " + path.getFileName()).asRuntimeException());
                    overallSuccess[0] = false;
                    return false;
                }
                currentlySendingFile = fileName;
                skipCurrentFileUpload = false;

                logger.info(String.format("Sending header for file %s to worker %s", path.getFileName(), workerId));
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
                            logger.info("Upload skipped for file " + fileName + " based on worker response.");
                            break;
                        }
                        if (!overallSuccess[0]) {
                            logger.warning("Aborting file " + fileName + " upload due to fatal error during transmission.");
                            break;
                        }

                        GridComms.FileChunk chunk = GridComms.FileChunk.newBuilder().setData(ByteString.copyFrom(buffer, 0, read)).build();
                        uploadObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setFileChunk(chunk).build());
                        bytesSent += read;
                    }
                    if (!skipCurrentFileUpload && overallSuccess[0]) {
                        logger.info(String.format("Sent %d bytes for file %s to worker %s", bytesSent, path.getFileName(), workerId));
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error sending chunk for file " + path.getFileName() + " to worker " + workerId, e);
                    overallSuccess[0] = false;
                    try {
                        uploadObserver.onError(Status.INTERNAL.withDescription("Distributor failed during file read/send: " + path.getFileName())
                                .withCause(e).asRuntimeException());
                    } catch (Exception observerException) {
                        logger.log(Level.SEVERE, "Failed to notify worker of send error.", observerException);
                    }
                    return false;
                } finally {
                    currentlySendingFile = null;
                }
            }
            if (overallSuccess[0]) {
                logger.info(String.format("All data sent for subtask %d/%d to worker %s. Completing upload stream.", taskId, subtaskId, workerId));
                uploadObserver.onCompleted();
            } else {
                logger.warning("Upload stream for subtask " + taskId + "/" + subtaskId + " to worker " + workerId + " not completed due to errors.");
            }

            try {
                logger.fine("Waiting for worker " + workerId + " response completion (latch)...");
                if (!latch.await(SUBTASK_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                    logger.severe(String.format("Worker %s did not respond completely in time for subtask %d/%d. Cancelling.", workerId, taskId, subtaskId));
                    overallSuccess[0] = false;
                }
            } catch (InterruptedException e) {
                logger.warning("Interrupted while waiting for worker response for subtask " + taskId + "/" + subtaskId);
                overallSuccess[0] = false;
                Thread.currentThread().interrupt();
            }

            logger.info(String.format("Data transmission ended for subtask %d/%d to worker %s, final success status: %s", taskId, subtaskId, workerId, overallSuccess[0]));
            return overallSuccess[0];

        } catch (Exception e) {
            logger.log(Level.SEVERE, String.format("Unexpected exception during data transmission for subtask %d/%d to worker %s: %s", taskId, subtaskId, workerId, e.getMessage()), e);
            return false;
        } finally {
            if (workerChannel != null) {
                logger.fine("Shutting down channel to worker " + workerId);
                workerChannel.shutdown();
                try {
                    if (!workerChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                        workerChannel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    workerChannel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                logger.fine("Channel to worker " + workerId + " shut down.");
            }
        }
    }

    private GridComms.WorkerAssignment requestWorkerFromManager(long taskId, long subtaskId) {
        ManagedChannel tempManagerChannel = null;
        try {
            tempManagerChannel = openManagerChannel();
            GridManagerGrpc.GridManagerBlockingStub tempManagerStub = GridManagerGrpc.newBlockingStub(tempManagerChannel); // Создаем заглушку

            GridComms.SubtaskRequest request = GridComms.SubtaskRequest.newBuilder()
                    .setTaskId(taskId)
                    .setSubtaskId(subtaskId)
                    .build();
            logger.info(String.format("Requesting worker for task %d/%d", taskId, subtaskId));
            return tempManagerStub.withDeadlineAfter(15, TimeUnit.SECONDS).requestWorker(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.SEVERE, "Error requesting worker: " + e.getStatus(), e);
            return GridComms.WorkerAssignment.newBuilder()
                    .setWorkerAvailable(false)
                    .setMessage("Error contacting manager: " + e.getStatus())
                    .build();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Failed to request worker due to unexpected error: " + e.getMessage(), e);
            return GridComms.WorkerAssignment.newBuilder()
                    .setWorkerAvailable(false)
                    .setMessage("Failed to request worker: " + e.getMessage())
                    .build();
        } finally {
            if (tempManagerChannel != null) {
                shutdownManagerChannel(tempManagerChannel);
            }
        }
    }

    private void checkRunningTasks() {
        logger.info("Start checking running tasks");
        var runningTasks = task.getResults().entrySet().stream()
                .filter(entry -> entry.getValue().getStatus().equals(SubtaskStatus.RUNNING))
                .toList();

        ManagedChannel tempManagerChannel = null;
        GridManagerGrpc.GridManagerBlockingStub tempManagerStub = null;
        boolean channelOpened = false;

        try {
            if (!runningTasks.isEmpty()) {
                tempManagerChannel = openManagerChannel();
                tempManagerStub = GridManagerGrpc.newBlockingStub(tempManagerChannel);
                channelOpened = true;
            } else {
                logger.info("No running tasks to check status for.");
                reassignFailedSubtasks();
                return;
            }

            for (Map.Entry<Long, Subtask> entry : runningTasks) {
                long subtaskId = entry.getValue().getId();
                if (!task.getResults().get(subtaskId).getStatus().equals(SubtaskStatus.RUNNING)) {
                    continue;
                }

                GridComms.WorkerStatusRequest request = GridComms.WorkerStatusRequest.newBuilder()
                        .setTaskId(task.getId())
                        .setSubtaskId(subtaskId)
                        .build();
                try {
                    logger.fine("Checking status for subtask " + task.getId() + "/" + subtaskId);
                    GridComms.WorkerStatusResponse response = tempManagerStub
                            .withDeadlineAfter(5, TimeUnit.SECONDS)
                            .getWorkerStatus(request);

                    GridComms.WorkerStatus currentWorkerStatus = response.getWorkerStatus();
                    logger.info("Manager reports status " + currentWorkerStatus + " for worker on subtask " + task.getId() + "/" + subtaskId);

                    if (currentWorkerStatus == GridComms.WorkerStatus.DEAD || currentWorkerStatus == GridComms.WorkerStatus.IDLE) {
                        logger.warning("Worker for subtask " + task.getId() + "/" + subtaskId + " reported as " + currentWorkerStatus + " by manager. Marking subtask as FAILED.");
                        Subtask failedTask = task.getResults().get(subtaskId);
                        ByteString data = (failedTask != null) ? failedTask.getData() : null;
                        if (failedTask != null && failedTask.getStatus() == SubtaskStatus.RUNNING) {
                            task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), data));
                        }
                    }
                } catch (StatusRuntimeException e) {
                    if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                        logger.warning(String.format("Manager reported NOT_FOUND for worker on subtask %d/%d. Assuming task failed or completed elsewhere.", task.getId(), subtaskId));
                        Subtask failedTask = task.getResults().get(subtaskId);
                        if (failedTask != null && failedTask.getStatus() == SubtaskStatus.RUNNING) {
                            task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), failedTask.getData()));
                        }
                    } else {
                        logger.log(Level.SEVERE, String.format("Error checking status via manager for subtask %d/%d: %s", task.getId(), subtaskId, e.getStatus()), e);
                    }
                } catch (IOException | ClassNotFoundException e) {
                    logger.log(Level.SEVERE, String.format("Error updating subtask %d/%d status locally after check: %s", task.getId(), subtaskId, e.getMessage()), e);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unexpected error during checkRunningTasks setup or loop: " + e.getMessage(), e);
        } finally {
            if (channelOpened && tempManagerChannel != null) {
                shutdownManagerChannel(tempManagerChannel);
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
                    logger.severe("Cannot reassign subtask " + entry.getValue().getId());
                    return;
                }
                logger.info(String.format("Reassigning failed subtask %d", subtaskId));
                submitSubtaskWithRetries(task.getId(), subtaskId, inputData);
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
