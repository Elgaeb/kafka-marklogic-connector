package com.marklogic.kafka.connect.integration.confluent.oracle;

import com.marklogic.kafka.connect.integration.AbstractIntegrationTest;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class CDCRelease2InsertTest extends AbstractIntegrationTest {

    protected static String DATABASE_NAME = "db1";
    protected static String SCHEMA_NAME = "SCHEMANAME";
    protected static String TABLE_NAME = "TABLENAME";
    protected static String TOPIC = DATABASE_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;

    protected Map<String, Object> dataValue(String type, Object value, String... flags) {
        return new HashMapBuilder<String, Object>()
            .with("type", type)
            .with("value", value)
            .with("flags", Arrays.asList(flags));
    }

    protected static int ID_ONE = 123;
    protected static int ID_TWO = 321;

    protected void testFirstValue(MarkLogicSinkTask task) throws IOException {
        HashMapBuilder<String, Object> values = new HashMapBuilder<String, Object>()
                .with("scn", "100")
                .with("op_type", "R")
                .with("table", TOPIC)
                .with("data", new HashMapBuilder<String, Object>()
                        .with("ID_ONE", dataValue("int64", ID_ONE, "PKEY"))
                        .with("ID_TWO", dataValue("int64", ID_TWO, "PKEY"))
                        .with("VALUE_ONE", dataValue("string", "llama"))
                        .with("VALUE_TWO", dataValue("string", "donkey"))
                );

        SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, values, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        String expectedUri = ("/myorg/" + SCHEMA_NAME + "/" + TABLE_NAME + "/" + ConfluentUtil.hash(Arrays.asList(ID_ONE, ID_TWO)) + ".json").toLowerCase();

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(expectedUri));
        assertThat(table.get("valueOne")).isEqualTo("llama");
        assertThat(table.get("valueTwo")).isEqualTo("donkey");

    }

    protected void testSecondValue(MarkLogicSinkTask task) throws IOException {
        HashMapBuilder<String, Object> values = new HashMapBuilder<String, Object>()
                .with("scn", "200")
                .with("op_type", "R")
                .with("table", TOPIC)
                .with("data", new HashMapBuilder<String, Object>()
                        .with("ID_ONE", dataValue("int64", ID_ONE, "PKEY"))
                        .with("ID_TWO", dataValue("int64", ID_TWO, "PKEY"))
                        .with("VALUE_TWO", dataValue("string", "sheep"))
                );

        SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, values, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        String expectedUri = ("/myorg/" + SCHEMA_NAME + "/" + TABLE_NAME + "/" + ConfluentUtil.hash(Arrays.asList(ID_ONE, ID_TWO)) + ".json").toLowerCase();

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(expectedUri));
        assertThat(table.get("valueOne")).isEqualTo("llama");
        assertThat(table.get("valueTwo")).isEqualTo("sheep");
    }

    protected void testThirdValue(MarkLogicSinkTask task) throws IOException {
        HashMapBuilder<String, Object> values = new HashMapBuilder<String, Object>()
                .with("scn", "150")
                .with("op_type", "R")
                .with("table", TOPIC)
                .with("data", new HashMapBuilder<String, Object>()
                        .with("ID_ONE", dataValue("int64", ID_ONE, "PKEY"))
                        .with("ID_TWO", dataValue("int64", ID_TWO, "PKEY"))
                        .with("VALUE_ONE", dataValue("string", "scorpion"))
                        .with("VALUE_TWO", dataValue("string", "pidgeon"))
                );

        SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, values, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        String expectedUri = ("/myorg/" + SCHEMA_NAME + "/" + TABLE_NAME + "/" + ConfluentUtil.hash(Arrays.asList(ID_ONE, ID_TWO)) + ".json").toLowerCase();

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(expectedUri));
        assertThat(table.get("valueOne")).isEqualTo("scorpion");
        assertThat(table.get("valueTwo")).isEqualTo("sheep");
    }

    @Test
    public void testDocumentUpdate() throws IOException {
        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(Collections.emptyMap(), "confluent-cdc-release2");
        task.start(config);

        testFirstValue(task);
        testSecondValue(task);
        testThirdValue(task);

        task.stop();
    }
}
