package com.marklogic.kafka.connect.sink.metadata;

import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

public class ConfluentOracleLOBSourceMetadataExtractor extends DefaultSourceMetadataExtractor {
    public static String COLUMN = "column";

    public ConfluentOracleLOBSourceMetadataExtractor(Map<String, Object> kafkaConfig) {
    }

    public Map<String, Object> extract(SinkRecord sinkRecord) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(TOPIC, sinkRecord.topic());

        final String keyStr = sinkRecord.key().toString();
        final String keyParts[] = keyStr.split("&");
        final SortedMap<String, String> keyColumnValues = new TreeMap<>();

        for(int i = 0; i < keyParts.length; i++) {
            final String part = keyParts[i];
            final String values[] = part.split("=");

            if(values[0].equalsIgnoreCase("tbl_col")) {
                String tableParts[] = values[1].split(".");

                meta.put(DefaultSourceMetadataExtractor.DATABASE, tableParts[0]);
                meta.put(DefaultSourceMetadataExtractor.SCHEMA, tableParts[1]);
                meta.put(DefaultSourceMetadataExtractor.TABLE, tableParts[2]);
                meta.put(COLUMN, tableParts[3]);
            } else {
                keyColumnValues.put(values[0], values[1]);
            }
        }

        final Collection<String> keyValues = keyColumnValues.values();
        final String id = ConfluentUtil.hash(keyValues);

        meta.put(ID, id);

        return meta;
    }
}
