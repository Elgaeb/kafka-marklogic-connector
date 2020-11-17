package com.marklogic.kafka.connect.sink.metadata;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfluentOracleSourceMetadataExtractor extends DefaultSourceMetadataExtractor {
    public ConfluentOracleSourceMetadataExtractor(Map<String, Object> kafkaConfig) {
    }

    protected Map<String, Object> extractId(SinkRecord sinkRecord) {
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();
        Map<String, Object> sourceMetadata = new HashMap<>();

        if(key != null) {
            sourceMetadata.put(ID, idFromSchemaAndValue(keySchema, key));
        } else {
            sourceMetadata.put(ID, UUID.randomUUID().toString());
        }

        return sourceMetadata;
    }

    protected static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile("^(.*)[.]([^.]+)[.]([^.]+)$");

    protected Map<String, Object> extractSourceMetadata(SinkRecord sinkRecord) {
        Struct value = (Struct)sinkRecord.value();
        Map<String, Object> meta = new HashMap<>();
        meta.put(TOPIC, sinkRecord.topic());

        String scn = value.getString("scn");
        if(scn != null) {
            meta.put("scn", Long.parseLong(scn));
        }

        meta.put("opType", value.getString("op_type"));
        meta.putAll(extractValuesFromTableName(value.getString("table")));
        return meta;
    }

    protected Map<String, Object> extractValuesFromTableName(String name) {
        Map<String, Object> meta = new HashMap<>();
        Matcher matcher = SCHEMA_NAME_PATTERN.matcher(name);
        if(matcher.matches()) {
            meta.put(DATABASE, matcher.group(1));
            meta.put(SCHEMA, matcher.group(2));
            meta.put(TABLE, matcher.group(3));
        }
        return meta;
    }

    public Map<String, Object> extract(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = new HashMap<>();

        sourceMetadata.putAll(extractId(sinkRecord));
        sourceMetadata.putAll(extractSourceMetadata(sinkRecord));
        sourceMetadata.put(TOPIC, sinkRecord.topic());

        return sourceMetadata;
    }
}
