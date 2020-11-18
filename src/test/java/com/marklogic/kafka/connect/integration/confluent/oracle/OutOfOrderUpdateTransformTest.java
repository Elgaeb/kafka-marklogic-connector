package com.marklogic.kafka.connect.integration.confluent.oracle;

import com.marklogic.kafka.connect.integration.AbstractIntegrationTest;
import com.marklogic.kafka.connect.sink.MarkLogicSinkTask;
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

public class OutOfOrderUpdateTransformTest extends AbstractIntegrationTest {

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

    protected static Struct newValue(Schema schema) {
        return new Struct(schema)
                .put("ID_ONE", 123)
                .put("ID_TWO", 321);
    }

    protected SinkRecord newValueSinkRecord(Schema schema, Struct value, long timestamp) {
        return new SinkRecord(TOPIC, 0, KEY_SCHEMA, KEY, schema, value, 0, timestamp, TimestampType.NO_TIMESTAMP_TYPE);
    }

    @Test
    public void testDocumentInsert() throws IOException {
        Schema valueSchema;

        MarkLogicSinkTask task = new MarkLogicSinkTask();
        Map<String, String> config = getOriginalConfiguration(Collections.emptyMap(), "confluent");
        task.start(config);

        valueSchema = SchemaBuilder.struct()
                .field("scn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("op_type", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ID_ONE", Schema.INT32_SCHEMA)
                .field("ID_TWO", Schema.INT32_SCHEMA)
                .field("VALUE_TWO", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        task.put(Collections.singleton(newValueSinkRecord(
                valueSchema,
                newValue(valueSchema)
                        .put("scn", "200")
                        .put("op_type", "U")
                        .put("table", TOPIC)
                        .put("VALUE_TWO", "updated_two"),
                200L)));
        task.flushAndWait(Collections.emptyMap());

        Map<String, Object> table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(EXPECTED_URI));
        assertThat(table.get("valueTwo")).isEqualTo("updated_two");

        valueSchema = SchemaBuilder.struct()
                .field("scn", Schema.OPTIONAL_STRING_SCHEMA)
                .field("op_type", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ID_ONE", Schema.INT32_SCHEMA)
                .field("ID_TWO", Schema.INT32_SCHEMA)
                .field("VALUE_ONE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("VALUE_TWO", Schema.OPTIONAL_STRING_SCHEMA)
                .field("VALUE_THREE", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        task.put(Collections.singleton(newValueSinkRecord(
                valueSchema,
                newValue(valueSchema)
                        .put("scn", "100")
                        .put("op_type", "C")
                        .put("table", TOPIC)
                        .put("VALUE_ONE", "one")
                        .put("VALUE_TWO", "two")
                        .put("VALUE_THREE", "three"),
                100L)));
        task.flushAndWait(Collections.emptyMap());

        table = extractInstance(SCHEMA_NAME, TABLE_NAME, readJsonDocument(EXPECTED_URI));
        assertThat(table.get("valueOne")).isEqualTo("one");
        assertThat(table.get("valueTwo")).isEqualTo("updated_two");
        assertThat(table.get("valueThree")).isEqualTo("three");

        task.stop();


    }
}
