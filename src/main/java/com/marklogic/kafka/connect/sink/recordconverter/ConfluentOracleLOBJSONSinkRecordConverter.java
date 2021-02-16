package com.marklogic.kafka.connect.sink.recordconverter;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.UpdateOperation;
import com.marklogic.kafka.connect.sink.metadata.DefaultSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.uri.URIFormatter;
import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import com.marklogic.kafka.connect.sink.util.UncheckedIOException;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

public class ConfluentOracleLOBJSONSinkRecordConverter implements SinkRecordConverter {

    protected static class Metadata {
        private final Map<String, ? extends Object> metadata;
        private final SortedMap<String, ? extends Object> keys;

        public Metadata(Map<String, ? extends Object> metadata, SortedMap<String, ? extends Object> keys) {
            this.metadata = metadata;
            this.keys = keys;
        }

        public Map<String, ? extends Object> getMetadata() {
            return metadata;
        }

        public SortedMap<String, ? extends Object> getKeys() {
            return keys;
        }
    }

    public static String COLUMN = "column";

    protected final List<String> collections;
    protected final String permissions;
    protected final CaseConverter collectionCaseConverter;
    protected final URIFormatter uriFormatter;
    protected final CaseConverter columnCaseConverter;
    protected final String collectionFormat;

    protected final JsonMapper mapper = new JsonMapper();

    public ConfluentOracleLOBJSONSinkRecordConverter(Map<String, Object> kafkaConfig) {
        this.columnCaseConverter = CaseConverter.ofType("camel");
        this.collectionFormat = (String) kafkaConfig.get(MarkLogicSinkConfig.CSRC_COLLECTION_FORMATSTRING);
        this.collections = (List<String>) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS);

        String permissions = (String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS);
        if (permissions != null && permissions.trim().length() > 0) {
            this.permissions = permissions.trim();
        } else {
            this.permissions = null;
        }

        this.collectionCaseConverter = CaseConverter.ofType((String) kafkaConfig.get(MarkLogicSinkConfig.CSRC_COLLECTION_CASE));

        this.uriFormatter = new URIFormatter(kafkaConfig);
    }

    public Metadata extractMetadata(SinkRecord sinkRecord) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SourceMetadataExtractor.TOPIC, sinkRecord.topic());

        final String keyStr = sinkRecord.key().toString();
        final String keyParts[] = keyStr.split("&");
        final SortedMap<String, Object> keyColumnValues = new TreeMap<>();

        for (int i = 0; i < keyParts.length; i++) {
            final String part = keyParts[i];
            final String values[] = part.split("=");

            if (values[0].equalsIgnoreCase("tbl_col")) {
                String tableParts[] = values[1].split("[.]");

                meta.put(DefaultSourceMetadataExtractor.DATABASE, tableParts[0]);
                meta.put(DefaultSourceMetadataExtractor.SCHEMA, tableParts[1]);
                meta.put(DefaultSourceMetadataExtractor.TABLE, tableParts[2]);
                meta.put(COLUMN, tableParts[3]);
            } else {
                final String uppercaseKeyName = values[0].toUpperCase(Locale.ROOT);
                if (uppercaseKeyName.endsWith("_ID")) {
                    // assume an _ID is a incrementing number
                    Object convertedValue = values[1];
                    try {
                        convertedValue = new BigInteger(values[1]);
                    } catch (NumberFormatException ex) {
                        // leave it as a string
                    }
                    keyColumnValues.put(this.columnCaseConverter.convert(uppercaseKeyName), convertedValue);
                } else {
                    keyColumnValues.put(values[0], values[1]);
                }
            }
        }

        final Collection<Object> keyValues = keyColumnValues.values();
        final String id = ConfluentUtil.hash(keyValues);

        meta.put(SourceMetadataExtractor.ID, id);

        meta.put("offset", sinkRecord.kafkaOffset());
        meta.put("partition", sinkRecord.kafkaPartition());

        return new Metadata(meta, keyColumnValues);
    }

    @Override
    public UpdateOperation convert(SinkRecord sinkRecord) {
        if (sinkRecord == null) {
            throw new NullPointerException("'record' must not be null.");
        }

        Object value = sinkRecord.value();

        if (sinkRecord == null) {
            throw new NullPointerException("'record' must have a value.");
        }

        try {
            Map<String, Object> convertedData = new HashMap<>();

            Metadata metadataPair = extractMetadata(sinkRecord);
            Map<String, ? extends Object> sourceMetadata = metadataPair.getMetadata();

            String base64Str = value.toString().trim().replaceAll("^['\"]|['\"]$", "");
            byte[] decodedBytes = Base64.getDecoder().decode(base64Str);
            String decodedString = new String(decodedBytes);
            convertedData.put(this.columnCaseConverter.convert(sourceMetadata.get(COLUMN).toString()), decodedString);

            metadataPair.getKeys().forEach((keyName, keyValue) -> {
                convertedData.put(keyName, keyValue);
            });

            String uri = this.uriFormatter.uri(sourceMetadata);
            AbstractWriteHandle writeHandle = toWriteHandle(convertedData, sinkRecord, sourceMetadata);
            DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, sourceMetadata);

            if (this.permissions != null) {
                new DefaultDocumentPermissionsParser().parsePermissions(this.permissions, documentMetadataHandle.getPermissions());
            }

            if (this.collections != null) {
                documentMetadataHandle.getCollections().addAll(this.collections);
            }

            return UpdateOperation.of(
                    Collections.singletonList(new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle)),
                    sinkRecord.kafkaPartition(),
                    sinkRecord.kafkaOffset()
            );
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected DocumentMetadataHandle toDocumentMetadata(SinkRecord sinkRecord, Map<String, ? extends Object> sourceMetadata) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (this.collectionFormat != null) {
            metadata.getCollections().add(this.collectionCaseConverter.convert(StringSubstitutor.replace(this.collectionFormat, sourceMetadata)));
        }
        return metadata;
    }

    protected AbstractWriteHandle toWriteHandle(Map<String, ? extends Object> valueMap, SinkRecord record, Map<String, ? extends Object> sourceMetadata) throws IOException {
        Map<String, Object> headers = new HashMap<>();
        headers.put("topic", record.topic());
        headers.put("timestamp", record.timestamp());
        headers.put("database", sourceMetadata.get(DefaultSourceMetadataExtractor.DATABASE));
        headers.put("offset", sourceMetadata.get("offset"));
        headers.put("partition", sourceMetadata.get("partition"));

        String schema = String.valueOf(sourceMetadata.get(DefaultSourceMetadataExtractor.SCHEMA)).toUpperCase();
        String table = String.valueOf(sourceMetadata.get(DefaultSourceMetadataExtractor.TABLE)).toUpperCase();

        headers.put("schema", schema);
        headers.put("table", table);

        Map<String, Object> envelope = new HashMap<>();
        envelope.put("headers", headers);
        envelope.put("instance", new HashMapBuilder<String, Object>().with(
                schema, new HashMapBuilder<String, Object>().with(
                        table, valueMap
                )
        ));

        Map<String, Object> root = new HashMap<>();
        root.put("envelope", envelope);

        String document = mapper.writeValueAsString(root);

        StringHandle content = new StringHandle()
                .with(document)
                .withFormat(Format.JSON);

        return content;

    }


}
