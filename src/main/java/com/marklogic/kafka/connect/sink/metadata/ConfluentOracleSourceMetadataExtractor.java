package com.marklogic.kafka.connect.sink.metadata;

import com.marklogic.kafka.connect.sink.util.ConfluentUtil;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConfluentOracleSourceMetadataExtractor extends DefaultSourceMetadataExtractor {
    public ConfluentOracleSourceMetadataExtractor(Map<String, Object> kafkaConfig) {
    }

    protected Map<String, Object> extractSourceMetadata(SinkRecord sinkRecord) {
        Struct value = (Struct)sinkRecord.value();
        Map<String, Object> meta = new HashMap<>();
        meta.put(TOPIC, sinkRecord.topic());

        String scn = value.getString("scn");
        if(scn != null) {
            meta.put("scn", Long.parseLong(scn));
        }

        meta.put("opType", value.getString("op_type"));
        meta.putAll(ConfluentUtil.extractValuesFromTableName(value.getString("table")));
        return meta;
    }


    public Map<String, Object> extract(SinkRecord sinkRecord) {
        Map<String, Object> sourceMetadata = new HashMap<>();

        sourceMetadata.putAll(ConfluentUtil.extractIdFromKey(sinkRecord));
        sourceMetadata.putAll(extractSourceMetadata(sinkRecord));
        sourceMetadata.put(TOPIC, sinkRecord.topic());

        return sourceMetadata;
    }
}
