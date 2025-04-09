package org.grid.distributor;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException {
        long taskId = System.currentTimeMillis();
        Distributor distributor = new Distributor("localhost", 9090, "data", taskId);

        Thread resultServerThread = new Thread(() -> {
            try {
                distributor.startResultServer();
                distributor.blockUntilResultServerShutdown();
            } catch (IOException | InterruptedException e) {
                System.out.println(e.getLocalizedMessage());;
            }
        });
        resultServerThread.start();

        distributor.runTask();

//        int totalSubtasks = (int) distributor.getTask().getSubtaskCount();

//        for (int i = 0; i < totalSubtasks; i++) {
//            boolean success = distributor.submitSubtask(0, i);
//            if (!success) {
//                System.out.printf("Изначальная отправка подзадачи %d не удалась. Она будет автоматически повторена.%n", i);
//            }
//        }
//
//        while (true) {
//            Map<Long, Subtask> results = distributor.getTask().getResults();
//            if (results.size() == totalSubtasks) {
//                boolean allDone = results.values().stream().allMatch(subtask -> subtask.getStatus() == SubtaskStatus.DONE);
//                if (allDone) {
//                    break;
//                }
//            }
//            System.out.println("Есть незавершённые подзадачи. Попытка повторного назначения неуспешных подзадач...");
//            distributor.reassignFailedSubtasks();
//            Thread.sleep(5000);
//        }
//
//        System.out.println("Все подзадачи завершены успешно. Останавливаем сервер приёма результатов.");
//        distributor.stopResultServer();
//        resultServerThread.join();
    }
}
