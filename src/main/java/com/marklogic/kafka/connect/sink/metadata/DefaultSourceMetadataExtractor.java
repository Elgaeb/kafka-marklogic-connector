package com.marklogic.kafka.connect.sink.metadata;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class DefaultSourceMetadataExtractor implements SourceMetadataExtractor {

    public static String TABLE = "table";
    public static String DATABASE = "database";
    public static String SCHEMA = "schema";
    public static String PREVIOUS_ID = "previousId";

    protected Map<String, Object> extractIdFromKey(SinkRecord sinkRecord) {
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();
        Map<String, Object> sourceMetadata = new HashMap<>();

        if(key != null) {
            sourceMetadata.put(ID, idFromSchemaAndValue(keySchema, key));
        } else {
            sourceMetadata.put(ID, UUID.randomUUID().toString());
        }

        return sourceMetadata;
    }

    protected String idFromSchemaAndValue(Schema valueSchema, Object value) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

            if (valueSchema.type().equals(Schema.Type.STRUCT)) {
                Struct valueStruct = (Struct) value;

                List<String> encodedValues = new ArrayList<>();
                for (Field field : valueSchema.fields()) {
                    String name = field.name();
                    Object valueObj = valueStruct.get(name);
                    // use a reserved character to cnostruct a substitute for a null value.
                    String idPart = valueObj == null ? "?null?" : URLEncoder.encode(valueObj.toString(), "UTF-8");
                    encodedValues.add(idPart);
                }

                String encodedValue = String.join("/", encodedValues);
                messageDigest.update(StandardCharsets.UTF_8.encode(encodedValue));
            } else {
                messageDigest.update(StandardCharsets.UTF_8.encode(URLEncoder.encode(value.toString(), "UTF-8")));
            }

            return String.format("%032x", new BigInteger(1, messageDigest.digest()));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Map<String, Object> extract(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = new HashMap<>();

        sourceMetadata.putAll(extractIdFromKey(sinkRecord));
        sourceMetadata.put(TOPIC, sinkRecord.topic());

        return sourceMetadata;
    }

}
