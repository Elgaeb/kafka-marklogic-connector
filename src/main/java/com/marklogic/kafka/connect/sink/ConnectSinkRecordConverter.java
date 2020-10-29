package com.marklogic.kafka.connect.sink;

import com.marklogic.client.document.DocumentWriteOperation;
import com.marklogic.client.ext.document.CaseConverter;
import com.marklogic.client.ext.document.ContentIdExtractor;
import com.marklogic.client.ext.util.DefaultDocumentPermissionsParser;
import com.marklogic.client.impl.DocumentWriteOperationImpl;
import com.marklogic.client.io.DocumentMetadataHandle;
import com.marklogic.client.io.Format;
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
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles converting a SinkRecord into a DocumentWriteOperation via the properties in the given config map.
 */
public class ConnectSinkRecordConverter implements SinkRecordConverter {

    private static final Logger logger = LoggerFactory.getLogger(ConnectSinkRecordConverter.class);

    private Converter converter;

    private final Pattern topicPattern;
    private final String uriFormat;
    private final String collectionFormat;
    private final ContentIdExtractor contentIdExtractor;

    public static final String CSRC_TOPIC_REGEX = "ml.connectSinkRecordConverter.topicRegex";
    public static final String CSRC_URI_FORMATSTRING = "ml.connectSinkRecordConverter.uriFormat";
    public static final String CSRC_URI_CASE = "ml.connectSinkRecordConverter.uriCase";
    public static final String CSRC_COLLECTION_FORMATSTRING = "ml.connectSinkRecordConverter.collectionFormat";
    public static final String CSRC_COLLECTION_CASE = "ml.connectSinkRecordConverter.collectionCase";

    private final List<String> collections;
    private final String permissions;

    private final CaseConverter uriCaseConverter;
    private final CaseConverter collectionCaseConverter;


    public ConnectSinkRecordConverter(Map<String, Object> kafkaConfig) {
        this.uriFormat = (String) kafkaConfig.getOrDefault(CSRC_URI_FORMATSTRING, "/%2$s.json");
        this.collectionFormat = (String) kafkaConfig.get(CSRC_COLLECTION_FORMATSTRING);

        String topicRegex = (String) kafkaConfig.get(CSRC_TOPIC_REGEX);
        if(topicRegex != null) {
            this.topicPattern = Pattern.compile(topicRegex);
        } else {
            this.topicPattern = null;
        }

        this.collections = (List<String>) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_COLLECTIONS);

        String permissions = (String) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_PERMISSIONS);
        if(permissions != null && permissions.trim().length() > 0) {
            this.permissions = permissions.trim();
        } else {
            this.permissions = null;
        }

        try {
            this.contentIdExtractor = (ContentIdExtractor) ((Class)kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_CONTENT_ID_EXTRACTOR)).newInstance();
        } catch(InstantiationException | IllegalAccessException ex) {
            throw new RuntimeException(ex);
        }

        this.uriCaseConverter = CaseConverter.ofType((String)kafkaConfig.get(CSRC_URI_CASE));
        this.collectionCaseConverter = CaseConverter.ofType((String)kafkaConfig.get(CSRC_COLLECTION_CASE));

        this.converter = new JsonConverter();
        Map<String, Object> config = new HashMap<>();
        config.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        config.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.converter.configure(config, false);
    }

    @Override
    public DocumentWriteOperation convert(SinkRecord sinkRecord) {
        String[] formatParameters = formatParameters(sinkRecord);
        String uri = toUri(sinkRecord, formatParameters);
        AbstractWriteHandle writeHandle = toWriteHandle(sinkRecord);
        DocumentMetadataHandle documentMetadataHandle = toDocumentMetadata(sinkRecord, formatParameters);

        if (this.permissions != null) {
            new DefaultDocumentPermissionsParser().parsePermissions(this.permissions, documentMetadataHandle.getPermissions());
        }

        if(this.collections != null) {
            documentMetadataHandle.getCollections().addAll(this.collections);
        }

        return new DocumentWriteOperationImpl(DocumentWriteOperation.OperationType.DOCUMENT_WRITE, uri, documentMetadataHandle, writeHandle);
    }

    protected DocumentMetadataHandle toDocumentMetadata(SinkRecord sinkRecord, Object[] formatParameters) {
        DocumentMetadataHandle metadata = new DocumentMetadataHandle();
        if(this.collectionFormat != null) {
            metadata.getCollections().add(this.collectionCaseConverter.convert(String.format(this.collectionFormat, formatParameters)));
        }
        return metadata;
    }

    protected AbstractWriteHandle toWriteHandle(SinkRecord record) {
        if ((record == null) || (record.value() == null)) {
            throw new NullPointerException("'record' must not be null, and must have a value.");
        }
        Object value = record.value();
        Schema valueSchema = record.valueSchema();

        final Schema headersSchema = SchemaBuilder.struct()
                .field("topic", Schema.OPTIONAL_STRING_SCHEMA)
                .field("timestamp", Schema.OPTIONAL_INT64_SCHEMA)
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

    protected String[] formatParameters(SinkRecord sinkRecord) {
        String id = contentIdExtractor.extractId(sinkRecord);
        String topic = sinkRecord.topic();
        if(topic == null || this.topicPattern == null) {
            return new String[]{ topic, id };
        } else {
            Matcher matcher = topicPattern.matcher(topic);
            if(matcher.matches()) {
                MatchResult matchResult = matcher.toMatchResult();
                int groupCount = matchResult.groupCount() + 1; // make sure to include group 0
                String[] matches = new String[groupCount + 1];
                for(int i = 0; i < groupCount; i++) {
                    matches[i] = matchResult.group(i);
                }
                matches[groupCount] = id;
                return matches;
            } else {
                return new String[]{ topic, id };
            }
        }
    }

    protected String toUri(SinkRecord sinkRecord, Object[] formatParameters) {
        return this.uriCaseConverter.convert(String.format(this.uriFormat, formatParameters));
    }
}
