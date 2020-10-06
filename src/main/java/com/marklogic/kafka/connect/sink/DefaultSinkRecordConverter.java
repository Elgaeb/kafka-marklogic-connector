package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
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

    public DefaultSinkRecordConverter(Map<String, String> kafkaConfig) {

        String val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS_ADD_TOPIC);
        if (val != null && val.trim().length() > 0) {
            addTopicToCollections = Boolean.parseBoolean(val.trim());
        }

        documentWriteOperationBuilder = new DocumentWriteOperationBuilder()
                .withCollections(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS))
                .withPermissions(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS))
                .withUriPrefix(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_PREFIX))
                .withUriSuffix(kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX));

        val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_FORMAT);
        if (val != null && val.trim().length() > 0) {
            format = Format.valueOf(val.toUpperCase());
        }
        val = kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_MIMETYPE);
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
        return documentWriteOperationBuilder.build(toContent(sinkRecord), addTopicToCollections(sinkRecord.topic(), addTopicToCollections));
    }

    /**
     * @param topic, addTopicToCollections
     * @return metadata
     */
    protected DocumentMetadataHandle addTopicToCollections(String topic, Boolean addTopicToCollections) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if (addTopicToCollections) {
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

//        logger.info("Got a new Record:");
//        logger.info(record.toString());
//        logger.info(record.value().getClass().getName());
//        logger.info(record.value().toString());

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
//            logger.info("Record is a Struct");
//            Schema valueSchema = record.valueSchema();
//            logger.info("Schema name: " + record.valueSchema().name());
//            logger.info("Schema: " + record.valueSchema().toString());

            StructWriteHandle content = new StructWriteHandle(this.converter)
                    .with(record.valueSchema(), (Struct) value)
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
