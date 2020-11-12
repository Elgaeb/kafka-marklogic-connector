package com.marklogic.kafka.connect.integration.debezium;

import com.marklogic.kafka.connect.integration.AbstractIntegrationTest;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

public abstract class AbstractDebeziumIntegrationTest extends AbstractIntegrationTest {
    protected static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .name("io.debezium.connector.oracle.Source")
            .field("version", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("name", SchemaBuilder.STRING_SCHEMA)
            .field("ts_ms", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("txId", SchemaBuilder.OPTIONAL_STRING_SCHEMA)
            .field("scn", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("commit_scn", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
            .field("snapshot", SchemaBuilder.OPTIONAL_BOOLEAN_SCHEMA)
            .build();

    protected SinkRecord createDebeziumMessage(
            String logicalDbName,
            String oracleSchemaName,
            String oracleTableName,
            Long scn,
            Long timestamp,
            String op,
            Struct keyStruct,
            Schema keySchema,
            Struct valueStruct,
            Schema valueSchema
    ) {

        String topicName = logicalDbName + "." + oracleSchemaName.toUpperCase() + "." + oracleTableName.toUpperCase();

        Schema dbValueSchema = SchemaBuilder.struct()
                .name(topicName + ".Envelope")
                .field("before", valueSchema)
                .field("after", valueSchema)
                .field("source", SOURCE_SCHEMA)
                .field("op", SchemaBuilder.STRING_SCHEMA)
                .field("ts_ms", SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                .build();

        Struct source = new Struct(SOURCE_SCHEMA);
        source.put("name", logicalDbName);
        source.put("ts_ms", timestamp);
        source.put("scn", scn);

        Struct dbValue = new Struct(dbValueSchema);
        dbValue.put("after", valueStruct);
        dbValue.put("source", source);
        dbValue.put("op", op);

        return new SinkRecord(topicName, 0, keySchema, keyStruct, dbValueSchema, dbValue, 0);
    }

}
