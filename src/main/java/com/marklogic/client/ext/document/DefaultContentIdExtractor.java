package com.marklogic.client.ext.document;

import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.UUID;

public class DefaultContentIdExtractor implements ContentIdExtractor {

	@Override
	public String extractId(SinkRecord sinkRecord) {
		return UUID.randomUUID().toString();
	}
}
