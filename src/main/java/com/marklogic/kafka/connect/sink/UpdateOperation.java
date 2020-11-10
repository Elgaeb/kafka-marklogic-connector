package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class UpdateOperation {

    protected final Set<String> deletes;
    protected final List<DocumentWriteOperation> writes;

    private UpdateOperation(Set<String> deletes, List<DocumentWriteOperation> writes) {
        this.deletes = deletes;
        this.writes = writes;
    }

    public List<DocumentWriteOperation> getWrites() {
        return writes;
    }

    public Set<String> getDeletes() {
        return deletes;
    }

    public static UpdateOperation of(List<DocumentWriteOperation> writes) {
        return new UpdateOperation(Collections.EMPTY_SET, writes);
    }

    public static UpdateOperation of(Set<String> deletes) {
        return new UpdateOperation(deletes, Collections.EMPTY_LIST);
    }

    public static UpdateOperation of(Set<String> deletes, List<DocumentWriteOperation> writes) {
        return new UpdateOperation(deletes, writes);
    }

    public static UpdateOperation noop() {
        return new UpdateOperation(Collections.EMPTY_SET, Collections.EMPTY_LIST);
    }
}
