package com.marklogic.kafka.connect.sink.metadata;

import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.apache.kafka.connect.sink.SinkRecord;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public interface SourceMetadataExtractor {
    public static String ID = "id";
    public static String TOPIC = "topic";

    Map<String, Object> extract(SinkRecord sinkRecord);

    static SourceMetadataExtractor newInstance(Map<String, Object> kafkaConfig) {
        SourceMetadataExtractor instance;
        Class<? extends SourceMetadataExtractor> sourceMetadataExtractorClass = ((Class<? extends SourceMetadataExtractor>) kafkaConfig.get(MarkLogicSinkConfig.DOCUMENT_SOURCE_METADATA_EXTRACTOR));
        try {
            instance = sourceMetadataExtractorClass.getConstructor(Map.class).newInstance(kafkaConfig);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException ex) {
            try {
                instance = sourceMetadataExtractorClass.newInstance();
            } catch (InstantiationException | IllegalAccessException iex) {
                throw new RuntimeException("No usable constructor for " + sourceMetadataExtractorClass.getName(), iex);
            }
        }

        return instance;
    }


}
