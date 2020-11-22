package com.marklogic.kafka.connect.sink.util;

import java.io.IOException;

public class UncheckedIOException extends RuntimeException {

    public UncheckedIOException(IOException ex) {
        super(ex);
    }
}
