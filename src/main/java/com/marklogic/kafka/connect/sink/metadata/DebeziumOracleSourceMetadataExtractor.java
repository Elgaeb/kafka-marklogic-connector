package com.marklogic.kafka.connect.sink.metadata;

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
                meta.put(PREVIOUS_ID, idFromSchemaAndValue(beforeSchema, before));
            }

            Object after = value.get("after");
            if(after != null) {
                Schema afterSchema = valueSchema.field("after").schema();
                meta.put(ID, idFromSchemaAndValue(afterSchema, after));
            }

            return meta;
        }
    }

    protected Map<String, Object> extractId(SinkRecord sinkRecord) {
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();

        if(key != null) {
            Map<String, Object> sourceMetadata = new HashMap<>();
            sourceMetadata.put(ID, idFromSchemaAndValue(keySchema, key));
            return sourceMetadata;
        } else {
            return extractIdFromValue(sinkRecord);
        }
    }

    protected static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile("^(.*)[.]([^.]+)[.]([^.]+)$");

    protected Map<String, Object> extractSourceMetadata(SinkRecord sinkRecord) {
        Map<String, Object> meta = new HashMap<>();

        // database, schema, table, scn?

        Schema valueSchema = sinkRecord.valueSchema();
        Schema afterSchema = valueSchema.field("after").schema();
        if(afterSchema != null) {
            meta.putAll(extractValuesFromSchemaName(afterSchema));
        } else {
            Schema beforeSchema = valueSchema.field("before").schema();
            if(beforeSchema != null) {
                meta.putAll(extractValuesFromSchemaName(beforeSchema));
            }
        }

        Struct value = (Struct)sinkRecord.value();
        Struct source = value.getStruct("source");

        Long scn = source.getInt64("scn");

        return meta;
    }

    protected Map<String, Object> extractValuesFromSchemaName(Schema schema) {
        Map<String, Object> meta = new HashMap<>();
        String name = schema.name(); // e.g. server1.DEBEZIUM.CUSTOMERS.Value
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
