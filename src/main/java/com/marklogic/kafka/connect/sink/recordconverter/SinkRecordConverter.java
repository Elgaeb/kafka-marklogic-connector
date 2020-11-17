package com.marklogic.kafka.connect.sink.recordconverter;

import com.marklogic.kafka.connect.sink.UpdateOperation;
import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Defines how a Kafka SinkRecord is converted into into a DocumentWriteOperation, which can then be
 * written to MarkLogic via a WriteBatcher or DocumentManager. Simplifies testing of this logic, as this avoids any
 * dependency on a running MarkLogic or running Kafka instance.
 */
public interface SinkRecordConverter {
	UpdateOperation convert(SinkRecord sinkRecord);
}
