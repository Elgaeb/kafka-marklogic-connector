package com.marklogic.client.ext.document;

import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.sink.SinkRecord;

public interface ContentIdExtractor {

	String extractId(SinkRecord sinkRecord);
	
}
