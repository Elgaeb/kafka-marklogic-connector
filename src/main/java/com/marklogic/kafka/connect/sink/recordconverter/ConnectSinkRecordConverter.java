package com.marklogic.kafka.connect.sink.recordconverter;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.uri.URIFormatter;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.StructWriteHandle;
import com.marklogic.kafka.connect.sink.UpdateOperation;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class ConnectSinkRecordConverter implements SinkRecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(ConnectSinkRecordConverter.class);

    protected Converter converter;

    protected final String collectionFormat;
    protected final SourceMetadataExtractor sourceMetadataExtractor;

    protected final List<String> collections;
    protected final String permissions;

    protected final CaseConverter collectionCaseConverter;
    protected final URIFormatter uriFormatter;

    public ConnectSinkRecordConverter(Map<String, Object> kafkaConfig) {
        this.collectionFormat = (String) kafkaConfig.get(MarkLogicSinkConfig.CSRC_COLLECTION_FORMATSTRING);

        this.collections = (List<String>) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS);

        String permissions = (String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS);
        if (permissions != null && permissions.trim().length() > 0) {
            this.permissions = permissions.trim();
        } else {
            this.permissions = null;
        }

        this.sourceMetadataExtractor = SourceMetadataExtractor.newInstance(kafkaConfig);

        this.collectionCaseConverter = CaseConverter.ofType((String) kafkaConfig.get(MarkLogicSinkConfig.CSRC_COLLECTION_CASE));

        this.uriFormatter = new URIFormatter(kafkaConfig);

        this.converter = new JsonConverter();
        Map<String, Object> config = new HashMap<>();
        config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.converter.configure(config, false);
    }

    @Override
    public UpdateOperation convert(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = this.sourceMetadataExtractor.extract(sinkRecord);
        String uri = this.uriFormatter.uri(sourceMetadata);
        AbstractWriteHandle writeHandle = toWriteHandle(sinkRecord, sourceMetadata);
        DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, sourceMetadata);

        if (this.permissions != null) {
            new DefaultDocumentPermissionsParser().parsePermissions(this.permissions, documentMetadataHandle.getPermissions());
        }

        if (this.collections != null) {
            documentMetadataHandle.getCollections().addAll(this.collections);
        }

        return UpdateOperation.of(Collections.singletonList(new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle)));
    }

    protected DocumentMetadataHandle toDocumentMetadata(SinkRecord sinkRecord, Map<String, Object> sourceMetadata) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (this.collectionFormat != null) {
            metadata.getCollections().add(this.collectionCaseConverter.convert(StringSubstitutor.replace(this.collectionFormat, sourceMetadata)));
        }
        return metadata;
    }

    protected AbstractWriteHandle toWriteHandle(SinkRecord record, Schema valueSchema, Object value, Map<String, Object> sourceMetadata) {
        if ((record == null) || (value == null) || valueSchema == null) {
            throw new NullPointerException("'record' must not be null, and must have a value and schema.");
        }

        final Schema headersSchema = SchemaBuilder.struct()
                .field("topic", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
                .field("scn", Schema.OPTIONAL_INT64_SCHEMA)
                .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
                .field("table", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .field("headers", headersSchema)
                .field("instance", valueSchema)
                .build();
        final Schema rootSchema = SchemaBuilder.struct()
                .field("envelope", envelopeSchema)
                .build();

        Struct headers = new Struct(headersSchema);
        headers.put("topic", record.topic());
        headers.put("timestamp", record.timestamp());
        headers.put("scn", sourceMetadata.get("scn"));
        headers.put("database", sourceMetadata.get("database"));
        headers.put("schema", String.valueOf(sourceMetadata.get("schema")).toUpperCase());
        headers.put("table", String.valueOf(sourceMetadata.get("table")).toUpperCase());

        Struct envelope = new Struct(envelopeSchema);
        envelope.put("headers", headers);
        envelope.put("instance", value);

        Struct root = new Struct(rootSchema);
        root.put("envelope", envelope);

        StructWriteHandle content = new StructWriteHandle(this.converter)
                .with(rootSchema, root)
                .withFormat(Format.JSON);

        return content;

    }

    protected AbstractWriteHandle toWriteHandle(SinkRecord record, Map<String, Object> sourceMetadata) {
        if (record == null) {
            throw new NullPointerException("'record' must not be null, and must have a value.");
        }

        return toWriteHandle(record, record.valueSchema(), record.value(), sourceMetadata);
    }

    public List<String> getCollections() {
        return collections;
    }

    public String getPermissions() {
        return permissions;
    }
}
