package com.marklogic.kafka.connect.sink.uri;

import com.marklogic.kafka.connect.sink.util.CaseConverter;
import com.marklogic.kafka.connect.sink.MarkLogicSinkConfig;
import org.apache.commons.text.StringSubstitutor;

import java.util.Map;

public class URIFormatter {

    private final String uriFormat;
    private final CaseConverter uriCaseConverter;

    public URIFormatter(Map<String, Object> kafkaConfig) {
        this.uriFormat = (String) kafkaConfig.getOrDefault(MarkLogicSinkConfig.CSRC_URI_FORMATSTRING, "${id}");
        this.uriCaseConverter = CaseConverter.ofType((String)kafkaConfig.get(MarkLogicSinkConfig.CSRC_URI_CASE));
    }

    public String uri(Map<String, Object> sourceMetadata) {
        return this.uriCaseConverter.convert(StringSubstitutor.replace(this.uriFormat, sourceMetadata));
    }
}
