package com.marklogic.kafka.connect.sink.metadata;

import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
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


    public Map<String, Object> extract(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = new HashMap<>();

        sourceMetadata.putAll(ConfluentUtil.extractIdFromKey(sinkRecord));
        sourceMetadata.put(TOPIC, sinkRecord.topic());

        return sourceMetadata;
    }

}
