package com.marklogic.kafka.connect.integration.dhf;

import com.marklogic.kafka.connect.integration.AbstractIntegrationTest;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class JSONInsertTest extends AbstractIntegrationTest  {
    protected static String TOPIC = "app-topic";

    protected static String KEY_VALUE = UUID.randomUUID().toString();
    protected static String KEY_VALUE_ENCODED = ConfluentUtil.hash(Collections.singletonList("" + KEY_VALUE));

    protected static String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    protected static String LOREM_IPSUM_BASE64 = Base64.getEncoder().encodeToString(LOREM_IPSUM.getBytes(StandardCharsets.UTF_8));

    protected static String EXPECTED_URI = MessageFormat.format("/myorg/{0}/{1}.json", TOPIC, KEY_VALUE_ENCODED).toLowerCase(Locale.ROOT);


    @Test
    public void testInsert() throws IOException {
        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(Collections.emptyMap(), "dhf-json");
        task.start(config);

        Map<String, Object> inputJson = new HashMapBuilder<String, Object>()
                .with("id", KEY_VALUE)
                .with("valueOne", 1)
                .with("valueTwo", "two")
                .with("nestedValue", new HashMapBuilder<String, Object>()
                        .with("nestedOne", "Nested Value One")
                        .with("nestedTwo", "Nested Value Two")
                );

        SinkRecord record = new SinkRecord(TOPIC, 0, null, KEY_VALUE, null, inputJson, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        Map<String, Object> json = readJsonDocument(EXPECTED_URI);

        Map<String, Object> envelope = (Map<String, Object>) json.get("envelope");
        assertThat(envelope).isNotNull();

        Map<String, Object> headers = (Map<String, Object>) envelope.get("headers");
        assertThat(headers).isNotNull();
        assertThat(headers).containsExactlyInAnyOrderEntriesOf(new HashMapBuilder<String, Object>()
                .with("partition", 0)
                .with("offset", 0)
                .with("topic", TOPIC)
                .with("timestamp", 100)
        );

        Map<String, Object> instance = (Map<String, Object>) envelope.get("instance");
        assertThat(instance).isNotNull();
        assertThat(instance).containsAllEntriesOf(new HashMapBuilder<String, Object>()
                .with("valueOne", 1)
                .with("valueTwo", "two")
                .with("id", KEY_VALUE)
        );
        assertThat(instance).containsOnlyKeys("nestedValue", "valueOne", "valueTwo", "id");

        Map<String, Object> nested = (Map<String, Object>) instance.get("nestedValue");
        assertThat(nested).isNotNull();
        assertThat(nested).containsExactlyInAnyOrderEntriesOf(new HashMapBuilder<String, Object>()
                .with("nestedOne", "Nested Value One")
                .with("nestedTwo", "Nested Value Two")
        );

        task.stop();
    }

}
