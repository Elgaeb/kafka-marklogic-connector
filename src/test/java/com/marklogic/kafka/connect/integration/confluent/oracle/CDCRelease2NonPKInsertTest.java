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

public class CDCRelease2NonPKInsertTest extends AbstractIntegrationTest {

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
                        .with("ID_ONE", dataValue("int64", ID_ONE))
                        .with("ID_TWO", dataValue("int64", ID_TWO))
                        .with("VALUE_ONE", dataValue("string", "llama"))
                        .with("VALUE_TWO", dataValue("string", "donkey"))
                );

        SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, values, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        String expectedUri = ("/myorg/" + SCHEMA_NAME + "/" + TABLE_NAME + "/" + ConfluentUtil.hash(ID_ONE, ID_TWO) + ".json").toLowerCase();

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(expectedUri));
        assertThat(table.get("valueOne")).isEqualTo("llama");
        assertThat(table.get("valueTwo")).isEqualTo("donkey");

    }

    @Test
    public void testDocumentInsert() throws IOException {
        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(
                new HashMapBuilder<String, String>()
                    .with(String.join(".", new String[] { "marklogic.confluent.oracle.keys", SCHEMA_NAME, TABLE_NAME }), "ID_ONE,ID_TWO"),
                "confluent-cdc-release2"
        );
        task.start(config);

        testFirstValue(task);

        task.stop();
    }
}
