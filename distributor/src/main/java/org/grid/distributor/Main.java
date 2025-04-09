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
                System.out.println(e.getLocalizedMessage());
            }
        });
        resultServerThread.start();

        distributor.runTask();

        distributor.shutdownScheduler();

        resultServerThread.join();
    }
}
