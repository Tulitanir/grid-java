package org.grid.distributor;

import org.grid.distributor.Distributor;
import org.grid.distributor.Subtask;
import org.grid.distributor.SubtaskStatus;

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

        // Вместо немедленного завершения фоновых процессов
        // ждем, пока все подзадачи не будут либо выполнены, либо окончательно провалены.
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

        resultServerThread.join();
    }
}
