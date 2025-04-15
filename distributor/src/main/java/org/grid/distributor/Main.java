package org.grid.distributor;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class Main {
    private static final String DEFAULT_MANAGER_HOST = "localhost";
    private static final int DEFAULT_MANAGER_PORT = 9090;
    private static final String DEFAULT_TASK_DIRECTORY = "data";
    private static final String DEFAULT_DISTRIBUTOR_HOST = "localhost";
    private static final int DEFAULT_RESULT_PORT = 10000;
    
    public static void main(String[] args) throws IOException, InterruptedException {
        long taskId = System.currentTimeMillis();
        String managerHost = DEFAULT_MANAGER_HOST;
        int managerPort = DEFAULT_MANAGER_PORT;
        String taskDirectory = DEFAULT_TASK_DIRECTORY;
        String distributorHost = DEFAULT_DISTRIBUTOR_HOST;
        int resultPort = DEFAULT_RESULT_PORT;

        try {
            if (args.length > 0) {
                managerHost = args[0];
            }
            if (args.length > 1) {
                managerPort = Integer.parseInt(args[1]);
            }
            if (args.length > 2) {
                taskDirectory = args[2];
            }
            if (args.length > 3) {
                distributorHost = args[3];
            }
            if (args.length > 4) {
                resultPort = Integer.parseInt(args[4]);
            }
        } catch (NumberFormatException e) {
            System.err.println("Ошибка: Неверный формат порта. Используйте целые числа.");
            printUsage();
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Ошибка при разборе аргументов: " + e.getMessage());
            printUsage();
            System.exit(1);
        }

        System.out.println("Используемые параметры:");
        System.out.println("  Manager Host: " + managerHost);
        System.out.println("  Manager Port: " + managerPort);
        System.out.println("  Task Directory: " + taskDirectory);
        System.out.println("  Distributor Host (Result Listener): " + distributorHost);
        System.out.println("  Distributor Port (Result Listener): " + resultPort);
        Distributor distributor = new Distributor(managerHost, managerPort, taskDirectory, distributorHost, resultPort, taskId);

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
        distributor.stopResultServer();

        resultServerThread.join();
        System.out.println("Final result: " + distributor.getTask().getFinalResult());

        Duration duration = Duration.ofMillis(System.currentTimeMillis() - taskId);
        long minutes = duration.toMinutes();
        long seconds = duration.minusMinutes(minutes).getSeconds();

        System.out.println("Task completion time: " + minutes + " mins. " + seconds + " sec.");
    }

    private static void printUsage() {
        System.err.println("\nИспользование: java org.grid.distributor.Main [managerHost] [managerPort] [taskDirectory] [distributorHost] [resultPort]");
        System.err.println("Примеры:");
        System.err.println("  java org.grid.distributor.Main");
        System.err.println("  java org.grid.distributor.Main manager.example.com 9090 /path/to/task_data my-distributor-host 12345");
        System.err.println("\nАргументы:");
        System.err.println("  managerHost      Хост менеджера (по умолчанию: " + DEFAULT_MANAGER_HOST + ")");
        System.err.println("  managerPort      Порт менеджера (по умолчанию: " + DEFAULT_MANAGER_PORT + ")");
        System.err.println("  taskDirectory    Директория с данными задачи (по умолчанию: " + DEFAULT_TASK_DIRECTORY + ")");
        System.err.println("  distributorHost  Хост, на котором дистрибьютор слушает результаты (по умолчанию: " + DEFAULT_DISTRIBUTOR_HOST + ")");
        System.err.println("  resultPort       Порт, на котором дистрибьютор слушает результаты (по умолчанию: " + DEFAULT_RESULT_PORT + ")");
        System.err.println();
    }
}
