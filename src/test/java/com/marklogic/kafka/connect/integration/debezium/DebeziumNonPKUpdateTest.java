package com.marklogic.kafka.connect.integration.debezium;

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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DebeziumNonPKUpdateTest extends AbstractDebeziumIntegrationTest {

    protected static final String LOGICAL_DB_NAME = "server1";
    protected static final String ORACLE_SCHEMA_NAME = "MY_SCHEMA";
    protected static final String ORACLE_TABLE_NAME = "MY_TABLE";

    @Test
    public void testDocumentInsert() throws IOException {

        MarkLogicSinkTask task = new MarkLogicSinkTask();

        Map<String, String> additionalConfig = new HashMap<>();
        Map<String, String> config = getOriginalConfiguration(additionalConfig, "debezium");
        task.start(config);

        Schema valueSchema = SchemaBuilder.struct()
                .name(LOGICAL_DB_NAME + "." + ORACLE_SCHEMA_NAME.toUpperCase() + "." + ORACLE_TABLE_NAME.toUpperCase())
                .field("VALUE_AS_STRING", SchemaBuilder.string().build())
                .build();
        Struct value = new Struct(valueSchema);
        value.put("VALUE_AS_STRING", "potato");

        SinkRecord record = createDebeziumMessage(LOGICAL_DB_NAME, ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, 10L, 100L, "c", null, null, value, valueSchema, null);
        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.EMPTY_MAP);

        String expectedUri = ("/myorg/" + ORACLE_SCHEMA_NAME + "/" + ORACLE_TABLE_NAME + "/3e2e95f5ad970eadfa7e17eaf73da97024aa5359.json").toLowerCase();
        Map<String, Object> table = extractInstance(ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, readJsonDocument(expectedUri));
        assertThat(table.get("valueAsString")).isEqualTo("potato");

        Struct newValue = new Struct(valueSchema);
        newValue.put("VALUE_AS_STRING", "radish");
        SinkRecord newRecord = createDebeziumMessage(LOGICAL_DB_NAME, ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, 10L, 100L, "c", null, null, newValue, valueSchema, value);
        task.put(Collections.singleton(newRecord));
        task.flushAndWait(Collections.EMPTY_MAP);
        task.stop();

        assertThatThrownBy(() -> { readJsonDocument(expectedUri); }).isInstanceOf(com.marklogic.client.ResourceNotFoundException.class);

        String newExpectedUri = ("/myorg/" + ORACLE_SCHEMA_NAME + "/" + ORACLE_TABLE_NAME + "/14d4eef6ee33c07f914caa3b48d2f0d4325b1762.json").toLowerCase();
        Map<String, Object> newTable = extractInstance(ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, readJsonDocument(newExpectedUri));

        assertThat(newTable.get("valueAsString")).isEqualTo("radish");
    }
}
