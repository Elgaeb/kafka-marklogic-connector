package com.marklogic.kafka.connect.integration;

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

public class UpdateTransformTest extends AbstractIntegrationTest {

    @Test
    public void testDocumentInsert() throws IOException {

        MarkLogicSinkTask task = new MarkLogicSinkTask();

        Map<String, String> additionalConfig = new HashMap<>();
        additionalConfig.put(MarkLogicSinkConfig.DMSDK_TRANSFORM, "kafka-update");
        Map<String, String> config = getOriginalConfiguration(additionalConfig);
        task.start(config);

        Schema keySchema = SchemaBuilder.struct()
                .field("ID_ONE", SchemaBuilder.int32().build())
                .field("ID_TWO", SchemaBuilder.int32().build())
                .build();
        Struct key = new Struct(keySchema);
        key.put("ID_ONE", 123);
        key.put("ID_TWO", 321);

        Schema valueSchema = SchemaBuilder.struct()
                .field("ID_ONE", SchemaBuilder.int32().build())
                .field("ID_TWO", SchemaBuilder.int32().build())
                .field("VALUE_AS_STRING", SchemaBuilder.string().build())
                .build();
        Struct value = new Struct(valueSchema);
        value.put("ID_ONE", 123);
        value.put("ID_TWO", 321);
        value.put("VALUE_AS_STRING", "potato");

        SinkRecord record = new SinkRecord("schemaname-tablename", 0, keySchema, key, valueSchema, value, 0);
        task.put(Collections.singleton(record));
        task.stop();

        String expectedUri = "/myorg/schemaname/tablename/7e1c0eda540e8a2715428974c05a3f57b3b0bd6f.json";
        Map<String, Object> document = readJsonDocument(expectedUri);
        assertThat(document).isNotNull();

        Map<String, Object> envelope = (Map<String, Object>) document.get("envelope");
        assertThat(envelope).isNotNull();

        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        assertThat(instance).isNotNull();

        Map<String, Object> schema = (Map<String, Object>) instance.get("SCHEMANAME");
        assertThat(instance).isNotNull();

        Map<String, Object> table = (Map<String, Object>) schema.get("TABLENAME");
        assertThat(instance).isNotNull();

        assertThat(table.get("idOne")).isEqualTo(123);
        assertThat(table.get("idTwo")).isEqualTo(321);
        assertThat(table.get("valueAsString")).isEqualTo("potato");
    }
}
