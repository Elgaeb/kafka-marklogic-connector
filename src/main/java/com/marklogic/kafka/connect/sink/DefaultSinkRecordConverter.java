package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.ContentIdExtractor;
import com.marklogic.client.ext.document.DocumentWriteOperationBuilder;
import com.marklogic.client.io.BytesHandle;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.io.marker.AbstractWriteHandle;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class DefaultSinkRecordConverter implements SinkRecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSinkRecordConverter.class);

    private DocumentWriteOperationBuilder documentWriteOperationBuilder;
    private Format format;
    private String mimeType;
    private Boolean addTopicToCollections = false;

    private Converter converter;

    public DefaultSinkRecordConverter(Map<String, Object> kafkaConfig) {
        addTopicToCollections = (Boolean) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC);
        try {
            documentWriteOperationBuilder = new DocumentWriteOperationBuilder()
                    .withCollections((List<String>) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS))
                    .withPermissions((String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS))
                    .withUriPrefix((String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX))
                    .withUriSuffix((String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX))
                    .withContentIdExtractor((ContentIdExtractor) ((Class)kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_CONTENT_ID_EXTRACTOR)).newInstance());
        } catch(InstantiationException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }

        String val = (String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_FORMAT);
        if (val != null && val.trim().length() > 0) {
            format = Format.valueOf(val.toUpperCase());
        }
        val = (String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE);
        if (val != null && val.trim().length() > 0) {
            mimeType = val;
        }

        this.converter = new JsonConverter();
        Map<String, Object> config = new HashMap<>();
        config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.converter.configure(config, false);
    }

    @Override
    public DocumentWriteOperation convert(SinkRecord sinkRecord) {
        return documentWriteOperationBuilder.build(
                sinkRecord,
                toContent(sinkRecord),
                addTopicToCollections(sinkRecord.topic(), addTopicToCollections)
        );
    }

    /**
     * @param topic, addTopicToCollections
     * @return metadata
     */
    protected DocumentMetadataHandle addTopicToCollections(String topic, Boolean addTopicToCollections) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (addTopicToCollections != null && addTopicToCollections) {
            metadata.getCollections().add(topic);
        }
        return metadata;
    }

    /**
     * Constructs an appropriate handle based on the value of the SinkRecord.
     *
     * @param record
     * @return
     */
    protected AbstractWriteHandle toContent(SinkRecord record) {
        if ((record == null) || (record.value() == null)) {
            throw new NullPointerException("'record' must not be null, and must have a value.");
        }
        Object value = record.value();
        if (value instanceof byte[]) {
            BytesHandle content = new BytesHandle((byte[]) value);
            if (format != null) {
                content.withFormat(format);
            }
            if (mimeType != null) {
                content.withMimetype(mimeType);
            }
            return content;
        }
        if (value instanceof Struct) {
            final Schema headersSchema = SchemaBuilder.struct()
                    .field("topicName", Schema.STRING_SCHEMA)
                    .build();
            final Schema envelopeSchema = SchemaBuilder.struct()
                    .field("headers", headersSchema)
                    .field("instance", record.valueSchema())
                    .build();
            final Schema rootSchema = SchemaBuilder.struct()
                    .field("envelope", envelopeSchema)
                    .build();

            Struct headers = new Struct(headersSchema);
            headers.put("topicName", record.topic());

            Struct envelope = new Struct(envelopeSchema);
            envelope.put("headers", headers);
            envelope.put("instance", value);

            Struct root = new Struct(rootSchema);
            root.put("envelope", envelope);

            StructWriteHandle content = new StructWriteHandle(this.converter)
                    .with(rootSchema, root)
//                    .with(record.valueSchema(), (Struct) value)
                    .withFormat(Format.JSON);
            return content;
        }

        StringHandle content = new StringHandle(record.value().toString());
        if (format != null) {
            content.withFormat(format);
        }
        if (mimeType != null) {
            content.withMimetype(mimeType);
        }
        return content;
    }

    public DocumentWriteOperationBuilder getDocumentWriteOperationBuilder() {
        return documentWriteOperationBuilder;
    }

    public void setDocumentWriteOperationBuilder(DocumentWriteOperationBuilder documentWriteOperationBuilder) {
        this.documentWriteOperationBuilder = documentWriteOperationBuilder;
    }
}
