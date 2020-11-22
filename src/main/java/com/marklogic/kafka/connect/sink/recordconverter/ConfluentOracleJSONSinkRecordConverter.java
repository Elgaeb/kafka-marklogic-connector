package com.marklogic.kafka.connect.sink.recordconverter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.StructWriteHandle;
import com.marklogic.kafka.connect.sink.UpdateOperation;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.uri.URIFormatter;
import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import com.marklogic.kafka.connect.sink.util.UncheckedIOException;
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

import java.io.IOException;
import java.io.StringReader;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class ConfluentOracleJSONSinkRecordConverter implements SinkRecordConverter {

    protected final Schema HEADERS_SCHEMA = SchemaBuilder.struct()
            .field("topic", Schema.OPTIONAL_STRING_SCHEMA)
            .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
            .field("scn", Schema.OPTIONAL_INT64_SCHEMA)
            .field("database", Schema.OPTIONAL_STRING_SCHEMA)
            .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .build();


    private static final Logger logger = LoggerFactory.getLogger(ConfluentOracleJSONSinkRecordConverter.class);

    protected Converter converter;

    protected final String collectionFormat;
    protected final SourceMetadataExtractor sourceMetadataExtractor;

    protected final List<String> collections;
    protected final String permissions;

    protected final CaseConverter collectionCaseConverter;
    protected final URIFormatter uriFormatter;

    protected final ObjectMapper mapper = new JsonMapper();

    public ConfluentOracleJSONSinkRecordConverter(Map<String, Object> kafkaConfig) {
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

    protected static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile("^(.*)[.]([^.]+)[.]([^.]+)$");

    @Override
    public UpdateOperation convert(SinkRecord sinkRecord) {
        if (sinkRecord == null) {
            throw new NullPointerException("'record' must not be null.");
        }

        Object value = sinkRecord.value();

        if (sinkRecord == null) {
            throw new NullPointerException("'record' must have a value.");
        }

        String valueJson = value.toString();

        try {
            JsonNode valueNode = mapper.readTree(new StringReader(valueJson));

            if(valueNode.has("payload")) {
                valueNode = valueNode.get("payload");
            }

            HashMapBuilder<String, Object> sourceMetadata = new HashMapBuilder<>();
            Optional.ofNullable(valueNode.get("scn")).ifPresent(scn -> sourceMetadata.with("scn", Long.parseUnsignedLong(scn.asText())));
            Optional.ofNullable(valueNode.get("op_type")).ifPresent(opType -> sourceMetadata.with("operation", opType.asText()));
            Optional.ofNullable(valueNode.get("table")).ifPresent(fullTableName -> {
                sourceMetadata.putAll(ConfluentUtil.extractValuesFromTableName(fullTableName.asText()));
            });

            sourceMetadata.putAll(ConfluentUtil.extractIdFromKey(sinkRecord));

            String uri = this.uriFormatter.uri(sourceMetadata);
            AbstractWriteHandle writeHandle = toWriteHandle(valueNode, sinkRecord, sourceMetadata);
            DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, sourceMetadata);

            if (this.permissions != null) {
                new DefaultDocumentPermissionsParser().parsePermissions(this.permissions, documentMetadataHandle.getPermissions());
            }

            if (this.collections != null) {
                documentMetadataHandle.getCollections().addAll(this.collections);
            }

            return UpdateOperation.of(Collections.singletonList(new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle)));
        } catch(IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected DocumentMetadataHandle toDocumentMetadata(SinkRecord sinkRecord, Map<String, Object> sourceMetadata) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (this.collectionFormat != null) {
            metadata.getCollections().add(this.collectionCaseConverter.convert(StringSubstitutor.replace(this.collectionFormat, sourceMetadata)));
        }
        return metadata;
    }

    protected AbstractWriteHandle toWriteHandle(JsonNode valueNode, SinkRecord record, Map<String, Object> sourceMetadata) throws IOException {
        Map<String, Object> values = new HashMap<>();
        valueNode.fields().forEachRemaining(entry -> {
            String name = entry.getKey();
            switch(name) {
                case "scn":
                case "op_type":
                case "table":
                    // discard these...
                    break;
                default:
                    values.put(name, entry.getValue());
                    break;
            }
        });


        Map<String, Object> headers = new HashMap<>();
        headers.put("topic", record.topic());
        headers.put("timestamp", record.timestamp());
        headers.put("scn", sourceMetadata.get("scn"));
        headers.put("database", sourceMetadata.get("database"));
        headers.put("schema", String.valueOf(sourceMetadata.get("schema")).toUpperCase());
        headers.put("table", String.valueOf(sourceMetadata.get("table")).toUpperCase());

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("headers", headers);
        envelope.put("instance", values);

        Map<String, Object> root = new HashMap<>();
        root.put("envelope", envelope);

        String document = mapper.writeValueAsString(root);

        StringHandle content = new StringHandle()
                .with(document)
                .withFormat(Format.JSON);

        return content;

    }

    public List<String> getCollections() {
        return collections;
    }

    public String getPermissions() {
        return permissions;
    }
}
