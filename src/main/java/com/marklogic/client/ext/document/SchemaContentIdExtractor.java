package com.marklogic.client.ext.document;

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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SchemaContentIdExtractor implements ContentIdExtractor {

    @Override
    public String extractId(SinkRecord sinkRecord) {
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();

        if(key != null) {
            try {
                MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

                if (keySchema.type().equals(Schema.Type.STRUCT)) {
                    Struct keyStruct = (Struct) key;

                    List<String> encodedValues = new ArrayList<>();
                    for (Field field : keySchema.fields()) {
                        String name = field.name();
                        String value = URLEncoder.encode(keyStruct.get(name).toString(), "UTF-8");
                        encodedValues.add(value);
                    }

                    String encodedValue = String.join("/", encodedValues);
                    messageDigest.update(StandardCharsets.UTF_8.encode(encodedValue));
                } else {
                    messageDigest.update(StandardCharsets.UTF_8.encode(URLEncoder.encode(key.toString(), "UTF-8")));
                }

                return String.format("%032x", new BigInteger(1, messageDigest.digest()));
            } catch (NoSuchAlgorithmException | UnsupportedEncodingException ex) {
                throw new RuntimeException(ex);
            }
        } else {
            return UUID.randomUUID().toString();
        }
    }
}
