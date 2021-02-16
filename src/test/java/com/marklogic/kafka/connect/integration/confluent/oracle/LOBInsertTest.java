package com.marklogic.kafka.connect.integration.confluent.oracle;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
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
import java.util.Base64;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class LOBInsertTest extends AbstractIntegrationTest {

    protected static String DATABASE_NAME = "db1";
    protected static String SCHEMA_NAME = "SCHEMANAME";
    protected static String TABLE_NAME = "TABLENAME";
    protected static String COLUMN_NAME = "COLUMN_CLOB";
    protected static String TOPIC = DATABASE_NAME + "." + SCHEMA_NAME + "." + TABLE_NAME;
    protected static int PK_VALUE = 55349754;
    protected static String PK_VALUE_ENCODED = ConfluentUtil.hash("" + PK_VALUE);

    protected static String LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    protected static String LOREM_IPSUM_BASE64 = Base64.getEncoder().encodeToString(LOREM_IPSUM.getBytes(StandardCharsets.UTF_8));

    protected static String EXPECTED_URI = MessageFormat.format("/myorg/{0}/{1}-{2}/{3}.json", SCHEMA_NAME, TABLE_NAME, COLUMN_NAME, PK_VALUE_ENCODED).toLowerCase(Locale.ROOT);


    @Test
    public void testInsert() throws IOException {
        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(Collections.emptyMap(), "confluent-lob");
        task.start(config);

        final String key = MessageFormat.format("PK_ID={0}&tbl_col={1}.{2}.{3}.{4}", "" + PK_VALUE, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COLUMN_NAME);

        SinkRecord record = new SinkRecord(TOPIC, 0, null, key, null, LOREM_IPSUM_BASE64, 0, 100L, TimestampType.NO_TIMESTAMP_TYPE);

        task.put(Collections.singleton(record));
        task.flushAndWait(Collections.emptyMap());

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(EXPECTED_URI));
        assertThat(table.get("columnClob")).isEqualTo(LOREM_IPSUM);

        task.stop();
    }

}
