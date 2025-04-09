package org.grid.distributor;

import com.google.protobuf.ByteString;

public class Subtask {
    private long id;
    private SubtaskStatus status;
    private Object result;
    private long startTime;
    private ByteString data;

    public Subtask(long id, SubtaskStatus status, Object result, long startTime) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.startTime = startTime;
    }

    public ByteString getData() {
        return data;
    }

    public Subtask(long id, SubtaskStatus status, Object result, long startTime, ByteString data) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.startTime = startTime;
        this.data = data;
    }

    public long getId() {
        return this.id;
    }

    public SubtaskStatus getStatus() {
        return status;
    }
}
