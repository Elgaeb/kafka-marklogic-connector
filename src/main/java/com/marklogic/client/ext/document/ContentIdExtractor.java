package com.marklogic.client.ext.document;

import org.apache.kafka.connect.sink.SinkRecord;

public interface ContentIdExtractor {

	String extractId(SinkRecord sinkRecord);

}
