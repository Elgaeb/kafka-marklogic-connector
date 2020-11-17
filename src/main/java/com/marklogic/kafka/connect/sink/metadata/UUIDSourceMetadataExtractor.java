package com.marklogic.kafka.connect.sink.metadata;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class UUIDSourceMetadataExtractor implements SourceMetadataExtractor {


    public UUIDSourceMetadataExtractor(Map<String, Object> kafkaConfig) {
    }

    public Map<String, Object> extract(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = new HashMap<>();

        sourceMetadata.put(ID, UUID.randomUUID().toString());
        sourceMetadata.put(TOPIC, sinkRecord.topic());

        return sourceMetadata;
    }
}
