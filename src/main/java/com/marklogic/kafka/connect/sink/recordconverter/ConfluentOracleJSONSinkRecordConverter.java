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
import com.marklogic.kafka.connect.sink.metadata.DefaultSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.uri.URIFormatter;
import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import com.marklogic.kafka.connect.sink.util.HashMapBuilder;
import com.marklogic.kafka.connect.sink.util.UncheckedIOException;
import org.apache.commons.text.StringSubstitutor;
import org.apache.kafka.connect.data.Decimal;
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
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    protected final JsonMapper mapper = new JsonMapper();

    protected final Map<String, SortedSet<String>> keyOverride;

    public ConfluentOracleJSONSinkRecordConverter(Map<String, Object> kafkaConfig) {
        this.columnCaseConverter = CaseConverter.ofType("camel");
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

        keyOverride = new HashMap<>();
        kafkaConfig.keySet().forEach(key -> {
            if(key.startsWith("marklogic.confluent.oracle.keys.")) {
                String keyParts[] = key.split("[.]");
                if(keyParts.length == 6) {
                    String schema = keyParts[4].toUpperCase();
                    String table = keyParts[5].toUpperCase();
                    String columns[] = ((String) kafkaConfig.get(key)).split("[,]");
                    SortedSet<String> columnSet = new TreeSet<>();
                    Arrays.stream(columns).map(this.columnCaseConverter::convert).forEach(columnSet::add);
                    keyOverride.put(schema + "." + table, columnSet);
                }
            }
        });
    }

    protected Map<String, ? extends Object> extractKeyFromData(SortedSet<String> keyColumns, Map<String, Object> data ) {
        try {
            List<String> encodedValues = new ArrayList<>();
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");
            keyColumns.forEach(column -> {
                Object valueObj = data.get(column);
                try {
                    String idPart = valueObj == null ? "?null?" : URLEncoder.encode(valueObj.toString(), "UTF-8");
                    encodedValues.add(idPart);
                } catch (UnsupportedEncodingException ex) {
                    // Java is required to support UTF-8 in all implementations
                    throw new RuntimeException(ex);
                }
            });
            String encodedValue = String.join("/", encodedValues);
            messageDigest.update(StandardCharsets.UTF_8.encode(encodedValue));
            return new HashMapBuilder<String, String>()
                .with(SourceMetadataExtractor.ID, String.format("%032x", new BigInteger(1, messageDigest.digest())))
                .with("plaintextId", encodedValue);
        }  catch (NoSuchAlgorithmException ex) {
            // Java is required to support SHA-1 in all implementations
            throw new RuntimeException(ex);
        }
    }

    protected CaseConverter columnCaseConverter;

    @Override
    public UpdateOperation convert(SinkRecord sinkRecord) {
        if (sinkRecord == null) {
            throw new NullPointerException("'record' must not be null.");
        }

        Object value = sinkRecord.value();

        if (sinkRecord == null) {
            throw new NullPointerException("'record' must have a value.");
        }

        if (! (value instanceof Map) ) {
            throw new IllegalArgumentException("value should be a 'Map'");
        }

        Map<String, Object> valueMap = (Map<String, Object>) value;

        try {
            if(valueMap.containsKey("payload")) {
                valueMap = (Map<String, Object>) valueMap.get("payload");
            }

            HashMapBuilder<String, Object> sourceMetadata = new HashMapBuilder<>();
            Optional.ofNullable(valueMap.get("scn")).ifPresent(scn -> sourceMetadata.with("scn", Long.parseUnsignedLong(scn.toString())));
            Optional.ofNullable(valueMap.get("op_type")).ifPresent(opType -> sourceMetadata.with("operation", opType));
            Optional.ofNullable(valueMap.get("table")).ifPresent(fullTableName -> {
                sourceMetadata.putAll(ConfluentUtil.extractValuesFromTableName(fullTableName.toString()));
            });

            final SortedSet<String> keyColumns = new TreeSet<>();
            HashMapBuilder<String, Object> convertedData = new HashMapBuilder<>();
            Optional.ofNullable(valueMap.get("data")).ifPresent(rawData -> {
                Map<String, Map<String, Object>> data = (Map) rawData;
                data.entrySet().forEach(dataEntry -> {
                    String columnName = this.columnCaseConverter.convert(dataEntry.getKey());

                    Map<String, Object> dataMap = dataEntry.getValue();
                    String type = (String) dataMap.get("type");

                    List<String> flags = Optional
                            .ofNullable((List<String>) dataMap.get("flags"))
                            .orElse(Collections.EMPTY_LIST);

                    flags = flags.stream()
                            .map(flag -> flag.toLowerCase())
                            .collect(Collectors.toList());

                    if(flags.contains("pkey")) {
                        keyColumns.add(columnName);
                    }

                    Object rawValue = dataMap.get("value");

                    switch(type) {
                        case "int8":
                        case "int16":
                        case "int32":
                        case "int64":
                        case "float32":
                        case "float64":
                        case "boolean":
                        case "string":
                            convertedData.put(columnName, rawValue);
                            break;
                        case "bytes":
//                            convertedData.put(columnName, bigIntegerFromBase64((String)rawValue));
                            convertedData.put(columnName, rawValue);
                            break;
                        case "array":
                        case "map":
                        case "struct":
                        default:
                            // these should not occur in the oracle cdc source
                            break;
                    }
                });
            });

            String schema = (String) sourceMetadata.get(DefaultSourceMetadataExtractor.SCHEMA);
            String table = (String) sourceMetadata.get(DefaultSourceMetadataExtractor.TABLE);

            sourceMetadata.putAll(extractKeyFromData(
                    Optional.ofNullable(keyOverride.get(schema + "." + table)).orElse(keyColumns),
                    convertedData
            ));

            String uri = this.uriFormatter.uri(sourceMetadata);
            AbstractWriteHandle writeHandle = toWriteHandle(convertedData, sinkRecord, sourceMetadata);
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

    protected AbstractWriteHandle toWriteHandle(Map<String, Object> valueMap, SinkRecord record, Map<String, Object> sourceMetadata) throws IOException {
        Map<String, Object> headers = new HashMap<>();
        headers.put("topic", record.topic());
        headers.put("timestamp", record.timestamp());
        headers.put("scn", sourceMetadata.get("scn"));
        headers.put("database", sourceMetadata.get("database"));

        String schema = String.valueOf(sourceMetadata.get("schema")).toUpperCase();
        String table = String.valueOf(sourceMetadata.get("table")).toUpperCase();

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

    public List<String> getCollections() {
        return collections;
    }

    public String getPermissions() {
        return permissions;
    }
}
