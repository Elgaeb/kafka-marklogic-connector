package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UpdateOperation {

    protected final Set<String> deletes;
    protected final List<DocumentWriteOperation> writes;

    protected long offset;
    protected Integer partition;

    private UpdateOperation(Set<String> deletes, List<DocumentWriteOperation> writes, Integer partition, long offset) {
        this.deletes = deletes;
        this.writes = writes;
        this.partition = partition;
        this.offset = offset;
    }

    public List<DocumentWriteOperation> getWrites() {
        return writes;
    }

    public Set<String> getDeletes() {
        return deletes;
    }

    public static UpdateOperation of(List<DocumentWriteOperation> writes, Integer partition, long offset) {
        return new UpdateOperation(Collections.EMPTY_SET, writes, partition, offset);
    }

    public static UpdateOperation of(Set<String> deletes, Integer partition, long offset) {
        return new UpdateOperation(deletes, Collections.EMPTY_LIST, partition, offset);
    }

    public static UpdateOperation of(Set<String> deletes, List<DocumentWriteOperation> writes, Integer partition, long offset) {
        return new UpdateOperation(deletes, writes, partition, offset);
    }

    public static UpdateOperation noop(Integer partition, long offset) {
        return new UpdateOperation(Collections.EMPTY_SET, Collections.EMPTY_LIST, partition, offset);
    }

    public Integer getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
