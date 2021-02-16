package com.marklogic.kafka.connect.sink.recordconverter;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.ext.util.DocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import com.marklogic.kafka.connect.sink.UpdateOperation;
import com.marklogic.kafka.connect.sink.uri.URIFormatter;
import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.UncheckedIOException;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DHFEnvelopeJSONSinkRecordConverter implements SinkRecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(DHFEnvelopeJSONSinkRecordConverter.class);

    protected final String collectionFormat;
    protected final List<String> collections;
    protected final String permissions;
    protected final CaseConverter collectionCaseConverter;
    protected final CaseConverter columnCaseConverter;
    protected final URIFormatter uriFormatter;

    protected final Converter converter;
    protected final JsonMapper mapper = new JsonMapper();

    public DHFEnvelopeJSONSinkRecordConverter(Map<String, Object> kafkaConfig) {
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

        this.converter = configureConverter();
    }

    protected Converter configureConverter() {
        Converter converter = new JsonConverter();
        Map<String, Object> config = new HashMap<>();
        config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        converter.configure(config, false);
        return converter;
    }

    protected DocumentMetadataHandle toDocumentMetadata(SinkRecord sinkRecord, Map<String, Object> sourceMetadata) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();

        if (this.collectionFormat != null) {
            metadata.getCollections().add(this.collectionCaseConverter.convert(StringSubstitutor.replace(this.collectionFormat, sourceMetadata)));
        }

        if (this.collections != null) {
            metadata.getCollections().addAll(this.collections);
        }

        if(this.permissions != null) {
            DocumentPermissionsParser permissionsParser = new DefaultDocumentPermissionsParser();
            permissionsParser.parsePermissions(this.permissions, metadata.getPermissions());
        }

        return metadata;
    }

    protected AbstractWriteHandle toWriteHandle(Object value, SinkRecord record, Map<String, Object> headers) {
        Map<String, Object> envelope = new HashMap<>();
        envelope.put("headers", headers);
        envelope.put("instance", value);

        Map<String, Object> root = new HashMap<>();
        root.put("envelope", envelope);

        try {

            String document = this.mapper.writeValueAsString(root);
            StringHandle content = new StringHandle()
                    .with(document)
                    .withFormat(Format.JSON);
            return content;

        } catch(IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public UpdateOperation convert(SinkRecord sinkRecord) {

        if (sinkRecord == null) {
            throw new NullPointerException("'record' must not be null.");
        }

        Object value = sinkRecord.value();

        Map<String, Object> headers = new HashMap<>();
        headers.put("topic", sinkRecord.topic());
        headers.put("timestamp", sinkRecord.timestamp());
        headers.put("offset", sinkRecord.kafkaOffset());
        headers.put("partition", sinkRecord.kafkaPartition());

        Map<String, Object> sourceMetadata = new HashMap<>(headers);
        sourceMetadata.put("id", ConfluentUtil.hash(Collections.singletonList(sinkRecord.key())));

        String uri = this.uriFormatter.uri(sourceMetadata);
        AbstractWriteHandle writeHandle = toWriteHandle(value, sinkRecord, headers);
        DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, sourceMetadata);

        return UpdateOperation.of(
                Collections.singletonList(new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle)),
                sinkRecord.kafkaPartition(),
                sinkRecord.kafkaOffset()
        );
    }
}
