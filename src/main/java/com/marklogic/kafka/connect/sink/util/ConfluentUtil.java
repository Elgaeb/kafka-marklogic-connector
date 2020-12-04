package com.marklogic.kafka.connect.sink.util;

import com.marklogic.kafka.connect.sink.metadata.DefaultSourceMetadataExtractor;
import com.marklogic.kafka.connect.sink.metadata.SourceMetadataExtractor;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfluentUtil {

    protected static final Pattern SCHEMA_NAME_PATTERN = Pattern.compile("^(.*)[.]([^.]+)[.]([^.]+)$");

    public static Map<String, Object> extractValuesFromTableName(String name) {
        Map<String, Object> meta = new HashMap<>();
        Matcher matcher = SCHEMA_NAME_PATTERN.matcher(name);
        if (matcher.matches()) {
            meta.put(DefaultSourceMetadataExtractor.DATABASE, matcher.group(1));
            meta.put(DefaultSourceMetadataExtractor.SCHEMA, matcher.group(2).toUpperCase());
            meta.put(DefaultSourceMetadataExtractor.TABLE, matcher.group(3).toUpperCase());
        }
        return meta;
    }

    public static Map<String, Object> extractIdFromKey(SinkRecord sinkRecord) {
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();
        Map<String, Object> sourceMetadata = new HashMap<>();

        if (key != null) {
            sourceMetadata.put(SourceMetadataExtractor.ID, idFromSchemaAndValue(keySchema, key));
        } else {
            sourceMetadata.put(SourceMetadataExtractor.ID, UUID.randomUUID().toString());
        }

        return sourceMetadata;
    }

    public static String idFromSchemaAndValue(Schema valueSchema, Object value) {
            List<String> values = new ArrayList<>();
            if (valueSchema.type().equals(Schema.Type.STRUCT)) {
                Struct valueStruct = (Struct) value;

                for (Field field : valueSchema.fields()) {
                    String name = field.name();
                    Object valueObj = valueStruct.get(field.name());
                    values.add(valueObj == null ? null : valueObj.toString());
                }
            } else if (value != null) {
                values.add(value.toString());
            } else {
                values.add(UUID.randomUUID().toString());
            }

            return hash(values);
    }

    public static String hash(Collection<String> values) {
        Object valuesArray[] = values.toArray(new String[values.size()]);
        return hash(valuesArray);
    }

    public static String hash(Object... values) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

            List<String> encodedValues = new ArrayList<>();
            for (Object valueObj : values) {
                String idPart = valueObj == null ? "?null?" : URLEncoder.encode(valueObj.toString(), "UTF-8");
                encodedValues.add(idPart);
            }

            String encodedValue = String.join("/", encodedValues);
            messageDigest.update(StandardCharsets.UTF_8.encode(encodedValue));

            return String.format("%032x", new BigInteger(1, messageDigest.digest()));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static BigInteger bigIntegerFromBase64(String base64) {
        return base64 == null ? null : new BigInteger(java.util.Base64.getDecoder().decode(base64));
    }
}
