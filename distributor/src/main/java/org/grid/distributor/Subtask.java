package org.grid.distributor;

import com.google.protobuf.ByteString;

public class Subtask {
    private long id;
    private SubtaskStatus status;
    private ByteString result;
    private long startTime;
    private long completionTime;
    private ByteString data;

    public Subtask(long id, SubtaskStatus status, ByteString result, long startTime) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.startTime = startTime;
    }

    public ByteString getData() {
        return data;
    }

    public Subtask(long id, SubtaskStatus status, ByteString result, long startTime, ByteString data) {
        this.id = id;
        this.status = status;
        this.result = result;
        this.startTime = startTime;
        this.data = data;
        this.completionTime = 0;
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

    public long getCompletionTime() {
        return completionTime;
    }

    public void setCompletionTime(long completionTime) {
        this.completionTime = completionTime;
    }
}
