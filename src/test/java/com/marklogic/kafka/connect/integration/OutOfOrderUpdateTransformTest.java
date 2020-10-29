package com.marklogic.kafka.connect.integration;

import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class OutOfOrderUpdateTransformTest extends AbstractIntegrationTest {

    protected Map<String, Object> getInstance(Map<String, Object> document, String schemaName, String tableName) {
        assertThat(document).isNotNull();

        Map<String, Object> envelope = (Map<String, Object>) document.get("envelope");
        assertThat(envelope).isNotNull();

        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        assertThat(instance).isNotNull();

        Map<String, Object> schema = (Map<String, Object>) instance.get(schemaName);
        assertThat(instance).isNotNull();

        Map<String, Object> table = (Map<String, Object>) schema.get(tableName);
        assertThat(instance).isNotNull();

        return table;
    }

    protected static Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field("ID_ONE", SchemaBuilder.int32().build())
            .field("ID_TWO", Schema.INT32_SCHEMA)
            .build();

    protected static Struct KEY = new Struct(KEY_SCHEMA).put("ID_ONE", 123).put("ID_TWO", 321);

    protected static String EXPECTED_URI = "/myorg/schemaname/tablename/7e1c0eda540e8a2715428974c05a3f57b3b0bd6f.json";
    protected static String TOPIC = "schemaname-tablename";
    protected static String SCHEMA_NAME = "SCHEMANAME";
    protected static String TABLE_NAME = "TABLENAME";

    protected static Struct newValue(Schema schema) {
        return new Struct(schema)
                .put("ID_ONE", 123)
                .put("ID_TWO", 321);
    }

    protected Map<String, Object> getValueDocument() throws IOException {
        return getInstance(readJsonDocument(EXPECTED_URI), SCHEMA_NAME, TABLE_NAME);
    }

    protected SinkRecord newValueSinkRecord(Schema schema, Struct value, long timestamp) {
        return new SinkRecord(TOPIC, 0, KEY_SCHEMA, KEY, schema, value, 0, timestamp, TimestampType.NO_TIMESTAMP_TYPE);
    }

    @Test
    public void testDocumentInsert() throws IOException {
        Schema valueSchema;

        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put(MarkLogicSinkConfig.DMSDK_TRANSFORM, "kafka-update");
        Map<String, String> config = getOriginalConfiguration(additionalConfig);
        task.start(config);

        valueSchema = SchemaBuilder.struct()
                .field("ID_ONE", Schema.INT32_SCHEMA)
                .field("ID_TWO", Schema.INT32_SCHEMA)
                .field("VALUE_TWO", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        task.put(Collections.singleton(newValueSinkRecord(
                valueSchema,
                newValue(valueSchema)
                        .put("VALUE_TWO", "updated_two"),
                200L)));
        task.flushAndWait(Collections.emptyMap());

        Map<String, Object> table = getValueDocument();
        assertThat(table.get("valueTwo")).isEqualTo("updated_two");

        valueSchema = SchemaBuilder.struct()
                .field("ID_ONE", Schema.INT32_SCHEMA)
                .field("ID_TWO", Schema.INT32_SCHEMA)
                .field("VALUE_ONE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("VALUE_TWO", Schema.OPTIONAL_STRING_SCHEMA)
                .field("VALUE_THREE", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        task.put(Collections.singleton(newValueSinkRecord(
                valueSchema,
                newValue(valueSchema)
                        .put("VALUE_ONE", "one")
                        .put("VALUE_TWO", "two")
                        .put("VALUE_THREE", "three"),
                100L)));
        task.flushAndWait(Collections.emptyMap());

        table = getValueDocument();
        assertThat(table.get("valueOne")).isEqualTo("one");
        assertThat(table.get("valueTwo")).isEqualTo("updated_two");
        assertThat(table.get("valueThree")).isEqualTo("three");

        task.stop();


    }
}
