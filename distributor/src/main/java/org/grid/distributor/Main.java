package org.grid.distributor;

public class Main {
    public static void main(String[] args) {
        var distributor = new Distributor("localhost", 50051, "data");
        distributor.submitSubtask(0, 2);
    }
}