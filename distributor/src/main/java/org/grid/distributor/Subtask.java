package org.grid.distributor;

public class Subtask {
    private long id;
    private SubtaskStatus status;
    private Object result;
    private long startTime;

    public Subtask(long id, SubtaskStatus status, Object result, long startTime) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.startTime = startTime;
    }

    public long getId() {
        return this.id;
    }

    public SubtaskStatus getStatus() {
        return status;
    }
}
