package com.marklogic.kafka.connect.sink;

import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public interface AlwaysVisibleRecommender extends ConfigDef.Recommender {
    default boolean visible(String name, Map<String, Object> parsedConfig) {
        return true;
    }
}
