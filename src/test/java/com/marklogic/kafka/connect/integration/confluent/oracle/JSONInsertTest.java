package com.marklogic.kafka.connect.integration.confluent.oracle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.marklogic.kafka.connect.integration.AbstractIntegrationTest;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import okio.Sink;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class JSONInsertTest extends AbstractIntegrationTest {

    protected static Schema KEY_SCHEMA = SchemaBuilder.struct()
            .field("ID_ONE", SchemaBuilder.int32().build())
            .field("ID_TWO", Schema.INT32_SCHEMA)
            .build();

    protected static Struct KEY = new Struct(KEY_SCHEMA).put("ID_ONE", 123).put("ID_TWO", 321);

    protected static String DATABASE_NAME = "db1";
    protected static String SCHEMA_NAME = "SCHEMANAME";
    protected static String TABLE_NAME = "TABLENAME";
    protected static String TOPIC = DATABASE_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;
    protected static String EXPECTED_URI = ("/myorg/" + SCHEMA_NAME + "/" + TABLE_NAME + "/7e1c0eda540e8a2715428974c05a3f57b3b0bd6f.json").toLowerCase();

    @Test
    public void testDocumentInsert() throws IOException {
        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(Collections.emptyMap(), "confluentjson");
        task.start(config);

        HashMapBuilder<String, Object> values = new HashMapBuilder<String, Object>()
                .with("scn", "100")
                .with("op_type", "R")
                .with("table", TOPIC)
                .with("ID_ONE", 123)
                .with("ID_TWO", 321)
                .with("VALUE_ONE", "llama");

        ObjectMapper mapper = new JsonMapper();
        String json = mapper.writeValueAsString(values);

        SinkRecord record = new SinkRecord(TOPIC, 0, KEY_SCHEMA, KEY, null, json, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(EXPECTED_URI));
        assertThat(table.get("valueOne")).isEqualTo("llama");

        task.stop();
    }

    @Test
    public void testDocumentInsertWithPayload() throws IOException {
        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(Collections.emptyMap(), "confluentjson");
        task.start(config);

        HashMapBuilder<String, Object> values = new HashMapBuilder<String, Object>()
                .with("scn", "100")
                .with("op_type", "R")
                .with("table", TOPIC)
                .with("ID_ONE", 123)
                .with("ID_TWO", 321)
                .with("VALUE_ONE", "llama");

        ObjectMapper mapper = new JsonMapper();
        String json = mapper.writeValueAsString(new HashMapBuilder<String, Object>().with("payload", values));

        SinkRecord record = new SinkRecord(TOPIC, 0, KEY_SCHEMA, KEY, null, json, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(EXPECTED_URI));
        assertThat(table.get("valueOne")).isEqualTo("llama");

        task.stop();
    }
}
