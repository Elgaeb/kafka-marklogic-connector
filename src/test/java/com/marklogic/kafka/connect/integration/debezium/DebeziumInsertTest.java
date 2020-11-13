package com.marklogic.kafka.connect.integration.debezium;

import com.marklogic.kafka.connect.integration.AbstractIntegrationTest;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
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

public class DebeziumInsertTest extends AbstractDebeziumIntegrationTest {

    protected static final String LOGICAL_DB_NAME = "server1";
    protected static final String ORACLE_SCHEMA_NAME = "MY_SCHEMA";
    protected static final String ORACLE_TABLE_NAME = "MY_TABLE";

    @Test
    public void testDocumentInsert() throws IOException {

        MarkLogicSinkTask task = new MarkLogicSinkTask();

        Map<String, String> additionalConfig = new HashMap<>();
        Map<String, String> config = getOriginalConfiguration(additionalConfig, "debezium");
        task.start(config);

        Schema keySchema = SchemaBuilder.struct()
                .field("ID_ONE", SchemaBuilder.int32().build())
                .field("ID_TWO", SchemaBuilder.int32().build())
                .build();
        Struct key = new Struct(keySchema);
        key.put("ID_ONE", 123);
        key.put("ID_TWO", 321);

        Schema valueSchema = SchemaBuilder.struct()
                .name(LOGICAL_DB_NAME + "." + ORACLE_SCHEMA_NAME.toUpperCase() + "." + ORACLE_TABLE_NAME.toUpperCase())
                .field("ID_ONE", SchemaBuilder.int32().build())
                .field("ID_TWO", SchemaBuilder.int32().build())
                .field("VALUE_AS_STRING", SchemaBuilder.string().build())
                .build();
        Struct value = new Struct(valueSchema);
        value.put("ID_ONE", 123);
        value.put("ID_TWO", 321);
        value.put("VALUE_AS_STRING", "potato");

        SinkRecord record = createDebeziumMessage(LOGICAL_DB_NAME, ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, 10L, 100L, "c", key, keySchema, value, valueSchema);
        task.put(Collections.singleton(record));
        task.stop();

        String expectedUri = ("/myorg/" + ORACLE_SCHEMA_NAME + "/" + ORACLE_TABLE_NAME + "/7e1c0eda540e8a2715428974c05a3f57b3b0bd6f.json").toLowerCase();
        Map<String, Object> table = extractInstance(ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, readJsonDocument(expectedUri));

        assertThat(table.get("idOne")).isEqualTo(123);
        assertThat(table.get("idTwo")).isEqualTo(321);
        assertThat(table.get("valueAsString")).isEqualTo("potato");
    }
}
