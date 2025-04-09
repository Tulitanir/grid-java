package org.grid.distributor;

import java.io.IOException;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        long taskId = System.currentTimeMillis();
        Distributor distributor = new Distributor("localhost", 9090, "data", taskId);

        Thread resultServerThread = new Thread(() -> {
            try {
                distributor.startResultServer();
                distributor.blockUntilResultServerShutdown();
            } catch (IOException | InterruptedException e) {
                System.err.println(e.getMessage());
            }
        });
        resultServerThread.start();

        distributor.runTask();

        boolean allDone = false;
        while (!allDone) {
            Map<Long, Subtask> results = distributor.getTask().getResults();
            allDone = results.values().stream().noneMatch(s -> s.getStatus() == SubtaskStatus.RUNNING || s.getStatus() == SubtaskStatus.FAILED);

            if (!allDone) {
                System.out.println("Есть незавершённые подзадачи, ожидаем...");
                Thread.sleep(5000);
            }
        }

        distributor.shutdownScheduler();
        distributor.shutdownManagerChannel();
        distributor.stopResultServer();

        resultServerThread.join();
    }
}
