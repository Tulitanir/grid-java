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
import java.util.Map.Entry;
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
    private static final int SUBTASK_TIMEOUT_MINUTES = 1;
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
        subtaskMonitoringScheduler.scheduleWithFixedDelay(this::checkRunningTasks, SUBTASK_TIMEOUT_MINUTES, SUBTASK_TIMEOUT_MINUTES, TimeUnit.SECONDS);
    }

    private void connectToManager() {
        System.out.println("Connecting to Manager at " + managerHost + ":" + managerPort);
        try {
            managerChannel = ManagedChannelBuilder.forAddress(managerHost, managerPort)
                    .usePlaintext()
                    .build();
            managerStub = GridManagerGrpc.newBlockingStub(managerChannel);
            System.out.println("Connected to Manager.");
        } catch (Exception e) {
            System.out.println("Failed to connect to manager during initialization: " + e.getLocalizedMessage());
            throw new RuntimeException("Distributor cannot start without connecting to manager", e);
        }
    }

    private void disconnectFromManager() {
        if (managerChannel != null && !managerChannel.isShutdown()) {
            System.out.println("Shutting down connection to Manager.");
            managerChannel.shutdown();
            try {
                if (!managerChannel.awaitTermination(5, TimeUnit.SECONDS)) {
                    managerChannel.shutdownNow();
                }
            } catch (InterruptedException e) {
                managerChannel.shutdownNow();
                Thread.currentThread().interrupt();
            } finally {
                managerChannel = null;
                managerStub = null;
            }
        }
    }

    public void shutdownScheduler() {
        System.out.println("Shutting down scheduled executor service...");
        subtaskMonitoringScheduler.shutdown();
        try {
            if (!subtaskMonitoringScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("Scheduled executor did not terminate in the given time. Forcing shutdown.");
                subtaskMonitoringScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            subtaskMonitoringScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public void startResultServer() throws IOException {
        if (resultServer == null || resultServer.isTerminated()) {
            System.out.println("Starting Result Receiver server on port " + resultPort);
            resultServer = ServerBuilder.forPort(resultPort)
                    .addService(new ResultReceiverImpl(task))
                    .build()
                    .start();
            System.out.println("Result Receiver server started.");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting down Result Receiver server due to JVM shutdown.");
                stopResultServer();
                System.out.println("Result Receiver server shut down.");
            }));
        } else {
            System.out.println("Result Receiver server is already running.");
        }
    }

    public void blockUntilResultServerShutdown() throws InterruptedException {
        if (resultServer != null) {
            resultServer.awaitTermination();
        }
    }

    public void stopResultServer() {
        if (resultServer != null && !resultServer.isShutdown()) {
            System.out.println("Stopping Result Receiver server...");
            resultServer.shutdown();
            subtaskMonitoringScheduler.shutdown();
            try {
                if (!resultServer.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Result Receiver server did not terminate gracefully. Forcing shutdown.");
                    resultServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                System.out.println("Interrupted while waiting for Result Receiver server shutdown: " + e.getLocalizedMessage());
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

        Object nextSubtask = null;
        long subtaskId = 0;
        while (subtaskIterator.hasNext()) {
            nextSubtask = subtaskIterator.next();
            try {
                ByteString inputDataBytes = Distributor.serializeToByteString(nextSubtask);
                subtaskId++;
                submitSubtask(taskId, subtaskId, inputDataBytes);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to serialize input data for task " + taskId + ", internal subtask ID " + subtaskId + ". Aborting task.", e);
                throw new RuntimeException(e);
            }
        }
    }

    public void submitSubtask(long taskId, long subtaskId, ByteString byteString) {
        int attempt = 0;
        boolean success = false;
        task.addResult(new Subtask(subtaskId, SubtaskStatus.RUNNING, null, System.currentTimeMillis(), byteString));
        while (attempt < MAX_RETRIES && !success) {
            System.out.printf("Submitting subtask %s/%s, attempt %d%n", taskId, subtaskId, attempt + 1);
            success = attemptSubmitSubtask(taskId, subtaskId, byteString);
            if (!success) {
                System.err.printf("Attempt %d for subtask %s/%s failed.%n", attempt + 1, taskId, subtaskId);
                attempt++;
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (!success) {
            System.err.printf("Subtask %s/%s failed after %d attempts. Marking as FAILED and scheduling reassignment.%n", taskId, subtaskId, MAX_RETRIES);
            task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis()));
        }
    }

    private boolean attemptSubmitSubtask(long taskId, long subtaskId, ByteString byteString) {
        String[] files = task.getFileNames();
        System.out.printf("Try to submit subtask %s/%s with files: %s%n", taskId, subtaskId, Arrays.toString(files));


        GridComms.WorkerAssignment workerAssignment = requestWorkerFromManager(taskId, subtaskId);
        if (!workerAssignment.getWorkerAvailable()) {
            System.out.println("No worker available from Manager for subtask " + taskId + "/" + subtaskId + ". Message: " + workerAssignment.getMessage());
            return false;
        }

        ManagedChannel channel = null;
        try {
            channel = ManagedChannelBuilder
                    .forAddress(workerAssignment.getAssignedWorker().getHost(), workerAssignment.getAssignedWorker().getPort())
                    .usePlaintext()
                    .build();

            SubtaskExchangeGrpc.SubtaskExchangeStub stub = SubtaskExchangeGrpc.newStub(channel);

            CountDownLatch countDownLatch = new CountDownLatch(1);
            final boolean[] overallSuccess = {true};

            StreamObserver<GridComms.SubtaskUploadResponse> responseStreamObserver = new StreamObserver<GridComms.SubtaskUploadResponse>() {
                @Override
                public void onNext(GridComms.SubtaskUploadResponse subtaskUploadResponse) {
                    if (subtaskUploadResponse.hasAck()) {
                        System.out.printf("Got acknowledgement from worker with subtask %s/%s: %s%n", taskId, subtaskId, subtaskUploadResponse.getAck().getMessage());
                    } else if (subtaskUploadResponse.hasFileStatus()) {
                        GridComms.FileStatus status = subtaskUploadResponse.getFileStatus();
                        System.out.printf("Worker with subtask %s/%s file status: %s, success: %s, detail: %s, bytes received: %s%n",
                                taskId, subtaskId, status.getFileName(), status.getSuccess(), status.getDetail(), status.getBytesReceived());
                        skipCurrentFileUpload = false;
                        if (currentlySendingFile != null &&
                                currentlySendingFile.equals(status.getFileName()) &&
                                String.format("File %s already exists%n", currentlySendingFile).equals(status.getDetail())) {
                            System.out.printf("Worker indicated file %s already exists. Aborting upload for this file.%n", status.getFileName());
                            skipCurrentFileUpload = true;
                        }
                    } else if (subtaskUploadResponse.hasFinalResult()) {
                        GridComms.OverallResult result = subtaskUploadResponse.getFinalResult();
                        System.out.printf("Worker with subtask %s/%s upload final result: Success=%s, FileProcessed=%s, Message=%s%n",
                                taskId, subtaskId, result.getOverallSuccess(), result.getFilesProcessedCount(), result.getFinalMessage());
                    } else if (subtaskUploadResponse.hasError()) {
                        GridComms.ErrorDetails errorDetails = subtaskUploadResponse.getError();
                        System.err.printf("Worker with subtask %s/%s got error: %s, related to file: %s%n",
                                taskId, subtaskId, errorDetails.getErrorMessage(), errorDetails.getFailedFileName());
                        if (errorDetails.getFatal()) {
                            overallSuccess[0] = false;
                        }
                    } else {
                        System.out.println("Received unknown response type from server.");
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    System.err.printf("Worker with subtask %s/%s got error: %s%n", taskId, subtaskId, Status.fromThrowable(throwable));
                    overallSuccess[0] = false;
                    countDownLatch.countDown();
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
                        .setDistributorHost(distributorHost)
                        .setDistributorPort(resultPort)
                        .setSubtaskInputData(byteString)
                        .build();

                uploadMessageStreamObserver.onNext(GridComms.SubtaskUploadMessage.newBuilder().setSubtaskMetadata(metadata).build());

                A:
                for (String fileName : files) {
                    Path path = Paths.get(taskDirectory, fileName);

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
                    try (InputStream inputStream = Files.newInputStream(path)) {
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
                if (!countDownLatch.await(SUBTASK_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                    System.err.printf("Worker didn't reply in %s minutes for subtask %s/%s%n", SUBTASK_TIMEOUT_MINUTES, taskId, subtaskId);
                    responseStreamObserver.onError(Status.CANCELLED.withDescription("Distributor timeout waiting for worker completion").asRuntimeException());
                    overallSuccess[0] = false;
                }
            } catch (InterruptedException e) {
                overallSuccess[0] = false;
                Thread.currentThread().interrupt();
            }
            System.out.printf("Task submission ended. ID: %s/%s, success: %s%n", taskId, subtaskId, overallSuccess[0]);
            return overallSuccess[0];
        } catch (Exception e) {
            System.err.printf("Exception during submission of subtask %s/%s: %s%n", taskId, subtaskId, e.getMessage());
            return false;
        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }

    private GridComms.WorkerAssignment requestWorkerFromManager(long taskId, long subtaskId) {
        if (managerStub == null) {
            System.out.println("Manager stub is not available!");
            connectToManager();
            if (managerStub == null) {
                throw new IllegalStateException("Cannot request worker, manager is unavailable.");
            }
        }
        GridComms.SubtaskRequest request = GridComms.SubtaskRequest.newBuilder()
                .setTaskId(taskId)
                .setSubtaskId(subtaskId)
                .build();
        System.out.println("Sending request to manager for worker for task " + taskId + "/" + subtaskId);
        try {
            return managerStub.withDeadlineAfter(15, TimeUnit.SECONDS).requestWorker(request);
        } catch (StatusRuntimeException e) {
            System.out.println("Failed to request worker from manager: " + e.getStatus() + " " + e.getLocalizedMessage());
            return GridComms.WorkerAssignment.newBuilder()
                    .setWorkerAvailable(false)
                    .setMessage("Failed to contact manager: " + e.getStatus())
                    .build();
        } finally {
            disconnectFromManager();
        }
    }

    private void checkRunningTasks() {
        System.out.println("Start checking running tasks");
        var runningTasks = task.getResults().entrySet().stream()
                .filter(entry -> entry.getValue().getStatus().equals(SubtaskStatus.RUNNING)).toList();

        connectToManager();
        try {
            for (Entry<Long, Subtask> task : runningTasks) {
                var subtaskId = task.getValue().getId();
                var request = GridComms.WorkerStatusRequest.newBuilder()
                        .setTaskId(this.task.getId())
                        .setSubtaskId(subtaskId)
                        .build();

                GridComms.WorkerStatusResponse response = managerStub.withDeadlineAfter(5, TimeUnit.SECONDS)
                        .getWorkerStatus(request);

                if (response.getWorkerStatus().equals(GridComms.WorkerStatus.DEAD) || response.getWorkerStatus().equals(GridComms.WorkerStatus.UNKNOWN)) {
                    var tasks = this.task.getResults();
                    var failedTask = tasks.get(subtaskId);
                    this.task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, null, System.currentTimeMillis(), failedTask.getData()));
                }
            }
        } finally {
            System.out.println("пиздец");
            disconnectFromManager();
            reassignFailedSubtasks();
        }
    }

    private void reassignFailedSubtasks() {
        System.out.println(task.getResults());
        for (Entry<Long, Subtask> entry : task.getResults().entrySet()) {
            if (entry.getValue().getStatus().equals(SubtaskStatus.FAILED)) {
                long subtaskId = entry.getKey();
                ByteString inputData = entry.getValue().getData();
                if (inputData == null)
                    throw new RuntimeException("Impossible to reassign subtask " + entry.getValue().getId());
                logger.info(String.format("Reassigning failed subtask %s%n", subtaskId));
                submitSubtask(task.getId(), subtaskId, inputData);
                logger.info(String.format("Reassignment of subtask %s%n", subtaskId));
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
            System.out.println("Received result for Task " + taskId + "/" + subtaskId + " from Worker " + workerId + ". Success: " + request.getSuccess() + ", Data: " + request.getResultData());

            GridComms.ResultAcknowledgement.Builder ackBuilder = GridComms.ResultAcknowledgement.newBuilder();

            if (task == null) {
                System.out.println("Received result for unknown or already completed Task ID: " + taskId);
                ackBuilder.setAcknowledged(false).setMessage("Task ID " + taskId + " not found or already completed.");
            } else {
                try {
                    Object resultObject = Distributor.deserialize(request.getResultData(), task.getTaskClassLoader());
                    System.out.println(resultObject);

                    if (!request.getSuccess()) {
                        System.out.printf("Result for subtask %s/%s indicates failure. Scheduling reassignment.%n", taskId, subtaskId);
                        task.addResult(new Subtask(subtaskId, SubtaskStatus.FAILED, resultObject, System.currentTimeMillis()));
                    } else {
                        task.addResult(new Subtask(subtaskId, SubtaskStatus.DONE, resultObject, System.currentTimeMillis()));
                    }
                } catch (IOException | ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }

                ackBuilder.setAcknowledged(true).setMessage("Result processed successfully.");
            }

            responseObserver.onNext(ackBuilder.build());
            responseObserver.onCompleted();
        }
    }

    private static Object deserialize(ByteString byteString, ClassLoader taskClassLoader) throws IOException, ClassNotFoundException {
        if (byteString == null || byteString.isEmpty()) {
            return null;
        }
        byte[] data = byteString.toByteArray();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new TaskSpecificObjectInputStream(bis, taskClassLoader)) {
            return ois.readObject();
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
            System.err.println("Class " + className + " not found in task classloader, trying super.resolveClass");
            try {
                return super.resolveClass(desc);
            } catch (ClassNotFoundException e2) {
                System.err.println("Class " + className + " not found in task classloader nor default classloader.");
                throw e2;
            }
        }
    }
}