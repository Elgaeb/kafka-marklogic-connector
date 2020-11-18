package com.marklogic.kafka.connect.sink.recordconverter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.kafka.connect.sink.metadata.DefaultSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.sink.StructWriteHandle;
import com.marklogic.kafka.connect.sink.UpdateOperation;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class DebeziumOracleSinkRecordConverter extends ConnectSinkRecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(DebeziumOracleSinkRecordConverter.class);

    public DebeziumOracleSinkRecordConverter(Map<String, Object> kafkaConfig) {
        super(kafkaConfig);
    }

    @Override
    protected AbstractWriteHandle toWriteHandle(SinkRecord record, Map<String, Object> sourceMetadata) {
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

        return toWriteHandle(record, afterSchema, afterValue, sourceMetadata);
    }

    @Override
    protected AbstractWriteHandle toWriteHandle(SinkRecord record, Schema valueSchema, Object value, Map<String, Object> sourceMetadata) {
        if ((record == null) || (value == null) || valueSchema == null) {
            throw new NullPointerException("'record' must not be null, and must have a value and schema.");
        }

        CaseConverter camelConverter = CaseConverter.ofType("camel");

        Struct valueStruct = (Struct)value;

        List<Triple<String, Schema, Object>> convertedFields = valueSchema.fields().stream()
                .map(field -> Triple.of(camelConverter.convert(field.name()), field.schema(), valueStruct.get(field)))
                .collect(Collectors.toList());

        SchemaBuilder convertedValueSchemaBuilder = SchemaBuilder.struct();
        convertedFields.forEach(triple -> convertedValueSchemaBuilder.field(triple.getLeft(), triple.getMiddle()));
        Schema convertedValueSchema = convertedValueSchemaBuilder.build();
        Struct convertedValue = new Struct(convertedValueSchema);
        convertedFields.forEach(triple -> convertedValue.put(triple.getLeft(), triple.getRight()));

        Struct message = (Struct) record.value();
        Struct source = message.getStruct("source");

        Long timestamp = source.getInt64("ts_ms");
        Long scn = source.getInt64("scn");

        String[] schemaNameParts = valueSchema.name().split("[.]");
        String oracleDatabaseName = schemaNameParts[0];
        String oraclSchemaName = schemaNameParts[1].toUpperCase();
        String oracleTableName = schemaNameParts[2].toUpperCase();

        final Schema headersSchema = SchemaBuilder.struct()
                .field("topic", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                .field("scn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Schema oracleSchemaSchema = SchemaBuilder.struct()
                .field(oracleTableName, convertedValueSchema)
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
        headers.put("timestamp", timestamp);
        headers.put("scn", scn);
        headers.put("database", oracleDatabaseName);
        headers.put("schema", oraclSchemaName);
        headers.put("table", oracleTableName);

        Struct oracleSchema = new Struct(oracleSchemaSchema);
        oracleSchema.put(oracleTableName, convertedValue);

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
        Map<String, Object> sourceMetadata = this.sourceMetadataExtractor.extract(sinkRecord);
        String uri = this.uriFormatter.uri(sourceMetadata);

        Object messageObject = sinkRecord.value();
        if(messageObject != null) {
            if(Struct.class.isInstance(messageObject)) {
                Struct message = (Struct) messageObject;
                String op = message.getString("op");

                switch(op) {
                    case "c": // create
                    case "r": // read
                    case "u": // update
                        DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, sourceMetadata);
                        Set<String> deletes = new HashSet<>();

                        if (this.getPermissions() != null) {
                            new DefaultDocumentPermissionsParser().parsePermissions(this.getPermissions(), documentMetadataHandle.getPermissions());
                        }

                        if(this.getCollections() != null) {
                            documentMetadataHandle.getCollections().addAll(this.getCollections());
                        }

                        if(sourceMetadata.get(DefaultSourceMetadataExtractor.PREVIOUS_ID) != null) {
                            String previousId = sourceMetadata.get(DefaultSourceMetadataExtractor.PREVIOUS_ID).toString();
                            Map<String, Object> previousMetadata = new HashMapBuilder<String, Object>()
                                    .with(sourceMetadata)
                                    .with(DefaultSourceMetadataExtractor.ID, previousId);
                            String previousUri = this.uriFormatter.uri(previousMetadata);
                            deletes.add(previousUri);
                        }

                        AbstractWriteHandle writeHandle = toWriteHandle(sinkRecord, sourceMetadata);
                        DocumentWriteOperation writeOperation = new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle);
                        List<DocumentWriteOperation> writeOperations = Collections.singletonList(writeOperation);

                        return UpdateOperation.of(deletes, writeOperations);
                    case "d": // delete
                        if(sourceMetadata.get(DefaultSourceMetadataExtractor.PREVIOUS_ID) != null) {
                            // this was an update to a non-PK table, uri will be gibberish
                            String previousId = sourceMetadata.get(DefaultSourceMetadataExtractor.PREVIOUS_ID).toString();
                            Map<String, Object> previousMetadata = new HashMapBuilder<String, Object>()
                                    .with(sourceMetadata)
                                    .with(DefaultSourceMetadataExtractor.ID, previousId);
                            uri = this.uriFormatter.uri(previousMetadata);
                        }
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
