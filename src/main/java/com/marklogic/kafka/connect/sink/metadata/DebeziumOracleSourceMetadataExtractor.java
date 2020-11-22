package com.marklogic.kafka.connect.sink.metadata;

import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DebeziumOracleSourceMetadataExtractor extends DefaultSourceMetadataExtractor {

    public DebeziumOracleSourceMetadataExtractor(Map<String, Object> kafkaConfig) {
    }

    protected Map<String, Object> extractIdFromValue(SinkRecord sinkRecord) {
        Struct value = (Struct)sinkRecord.value();
        Schema valueSchema = sinkRecord.valueSchema();

        if(value == null) {
            // tombstone record?
            return Collections.emptyMap();
        } else {
            Map<String, Object> meta = new HashMap<>();

            Object before = value.get("before");
            if(before != null) {
                Schema beforeSchema = valueSchema.field("before").schema();
                meta.put(PREVIOUS_ID, ConfluentUtil.idFromSchemaAndValue(beforeSchema, before));
            }

            Object after = value.get("after");
            if(after != null) {
                Schema afterSchema = valueSchema.field("after").schema();
                meta.put(ID, ConfluentUtil.idFromSchemaAndValue(afterSchema, after));
            }

            return meta;
        }
    }

    protected Map<String, Object> extractId(SinkRecord sinkRecord) {
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();

        if(key != null) {
            Map<String, Object> sourceMetadata = new HashMap<>();
            sourceMetadata.put(ID, ConfluentUtil.idFromSchemaAndValue(keySchema, key));
            return sourceMetadata;
        } else {
            return extractIdFromValue(sinkRecord);
        }
    }

    protected static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile("^(.*)[.]([^.]+)[.]([^.]+)[.]Value$");

    protected Map<String, Object> extractSourceMetadata(SinkRecord sinkRecord) {
        Struct value = (Struct)sinkRecord.value();
        Struct source = value.getStruct("source");

        return new HashMapBuilder<String, Object>()
                .with("name", source.getString("name"))
                .with("database", source.getString("db"))
                .with("schema", source.getString("schema").toUpperCase())
                .with("table", source.getString("table").toUpperCase());
    }

    public Map<String, Object> extract(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = new HashMap<>();

        sourceMetadata.putAll(extractId(sinkRecord));
        sourceMetadata.putAll(extractSourceMetadata(sinkRecord));
        sourceMetadata.put(TOPIC, sinkRecord.topic());

        return sourceMetadata;
    }
}
