package com.marklogic.kafka.connect.integration;

import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class SchemaContentIdExtractorTest extends AbstractIntegrationTest {

    @Test
    public void testDocumentInsert() throws IOException {

        MarkLogicSinkTask task = new MarkLogicSinkTask();

        Map<String, String> config = getOriginalConfiguration();
        task.start(config);

        Schema keySchema = SchemaBuilder.struct()
                .field("id1", SchemaBuilder.int32().build())
                .field("id2", SchemaBuilder.int32().build())
                .build();
        Struct key = new Struct(keySchema);
        key.put("id1", 123);
        key.put("id2", 321);

        Schema valueSchema = SchemaBuilder.struct()
                .field("id1", SchemaBuilder.int32().build())
                .field("id2", SchemaBuilder.int32().build())
                .field("value", SchemaBuilder.string().build())
                .build();
        Struct value = new Struct(valueSchema);
        value.put("id1", 123);
        value.put("id2", 321);
        value.put("value", "potato");

        SinkRecord record = new SinkRecord("test-topic", 0, keySchema, key, valueSchema, value, 0);
        task.put(Collections.singleton(record));
        task.stop();

        String expectedUri = "/myorg/test/topic/7e1c0eda540e8a2715428974c05a3f57b3b0bd6f.json";
        Map<String, Object> document = readJsonDocument(expectedUri);
        assertThat(document).isNotNull();

        Map<String, Object> envelope = (Map<String, Object>) document.get("envelope");
        assertThat(envelope).isNotNull();

        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        assertThat(instance).isNotNull();

        assertThat(instance.get("id1")).isEqualTo(123);
        assertThat(instance.get("id2")).isEqualTo(321);
        assertThat(instance.get("value")).isEqualTo("potato");
    }
}
