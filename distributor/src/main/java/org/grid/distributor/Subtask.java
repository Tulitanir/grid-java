package org.grid.distributor;

import com.google.protobuf.ByteString;

public class Subtask {
    private long id;
    private SubtaskStatus status;
    private ByteString result;
    private long executionTime;
    private ByteString data;

    public Subtask(long id, SubtaskStatus status, ByteString result) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.executionTime = 0;
    }

    public ByteString getData() {
        return data;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public Subtask(long id, SubtaskStatus status, ByteString result, long startTime, ByteString data) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.data = data;
        this.executionTime = 0;
    }

    public long getId() {
        return this.id;
    }

    public SubtaskStatus getStatus() {
        return status;
    }

    public ByteString getResult() {
        return result;
    }


    public void setResult(ByteString result) {
        this.result = result;
    }

    public void setStatus(SubtaskStatus status) {
        this.status = status;
    }
}
