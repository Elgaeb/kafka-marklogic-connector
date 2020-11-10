package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class DebeziumOracleSinkRecordConverter extends ConnectSinkRecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(DebeziumOracleSinkRecordConverter.class);

    public DebeziumOracleSinkRecordConverter(Map<String, Object> kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected AbstractWriteHandle toWriteHandle(SinkRecord record) {
        Object rawValue = record.value();
        if(!Struct.class.isInstance(rawValue)) {
            throw new IllegalArgumentException("schema registry must be enabled.");
        }

        Struct value = (Struct) rawValue;
        Schema valueSchema = record.valueSchema();

        Field afterField = valueSchema.field("after");
        Schema afterSchema = afterField.schema();
        Object afterValue = value.get(afterField);

        if(afterSchema == null || afterValue == null) {
            throw new NullPointerException("'after' must not be null and must have a schema.");
        }

        return toWriteHandle(record, afterSchema, afterValue);
    }

    @Override
    protected AbstractWriteHandle toWriteHandle(SinkRecord record, Schema valueSchema, Object value) {
        if ((record == null) || (value == null) || valueSchema == null) {
            throw new NullPointerException("'record' must not be null, and must have a value and schema.");
        }

        String[] schemaNameParts = valueSchema.name().split("[.]");
        String oracleDatabaseName = schemaNameParts[0];
        String oraclSchemaName = schemaNameParts[1].toUpperCase();
        String oracleTableName = schemaNameParts[2].toUpperCase();

        final Schema headersSchema = SchemaBuilder.struct()
                .field("topic", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Schema oracleSchemaSchema = SchemaBuilder.struct()
                .field(oracleTableName, valueSchema)
                .build();
        final Schema instanceSchema = SchemaBuilder.struct()
                .field(oraclSchemaName, oracleSchemaSchema)
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .field("headers", headersSchema)
                .field("instance", instanceSchema)
                .build();
        final Schema rootSchema = SchemaBuilder.struct()
                .field("envelope", envelopeSchema)
                .build();

        Struct headers = new Struct(headersSchema);
        headers.put("topic", record.topic());
        headers.put("timestamp", record.timestamp());
        headers.put("database", oracleDatabaseName);
        headers.put("schema", oraclSchemaName);
        headers.put("table", oracleTableName);

        Struct oracleSchema = new Struct(oracleSchemaSchema);
        oracleSchema.put(oracleTableName, value);

        Struct instance = new Struct(instanceSchema);
        instance.put(oraclSchemaName, oracleSchema);

        Struct envelope = new Struct(envelopeSchema);
        envelope.put("headers", headers);
        envelope.put("instance", instance);

        Struct root = new Struct(rootSchema);
        root.put("envelope", envelope);

        StructWriteHandle content = new StructWriteHandle(this.converter)
                .with(rootSchema, root)
                .withFormat(Format.JSON);

        return content;
    }


    @Override
    public UpdateOperation convert(SinkRecord sinkRecord) {
        String[] formatParameters = formatParameters(sinkRecord);
        String uri = toUri(sinkRecord, formatParameters);

        Object messageObject = sinkRecord.value();
        if(messageObject != null) {
            if(Struct.class.isInstance(messageObject)) {
                Struct message = (Struct) messageObject;
                String op = message.getString("op");

                switch(op) {
                    case "c": // create
                    case "r": // read
                    case "u": // update
                        DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, formatParameters);

                        if (this.getPermissions() != null) {
                            new DefaultDocumentPermissionsParser().parsePermissions(this.getPermissions(), documentMetadataHandle.getPermissions());
                        }

                        if(this.getCollections() != null) {
                            documentMetadataHandle.getCollections().addAll(this.getCollections());
                        }


                        AbstractWriteHandle writeHandle = toWriteHandle(sinkRecord);
                        DocumentWriteOperation writeOperation = new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle);
                        List<DocumentWriteOperation> writeOperations = Collections.singletonList(writeOperation);

                        return UpdateOperation.of(writeOperations);
                    case "d": // delete
                        return UpdateOperation.of(Collections.singleton(uri));
                    default:
                        throw new IllegalArgumentException("op must be one of [ 'c', 'r', 'u', 'd' ]");
                }
            } else {
                throw new IllegalArgumentException("schema registry must be enabled.");
            }
        } else {
            // tombstone message
            return UpdateOperation.noop();
        }

    }
}
