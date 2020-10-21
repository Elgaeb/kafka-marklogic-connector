package com.marklogic.client.ext.document;

import org.apache.commons.text.CaseUtils;
import org.apache.commons.text.WordUtils;

public interface CaseConverter {

    String convert(String input);

    char[] delimiters = new char[] {
            '-',
            '_',
            '.',
    };

    static CaseConverter ofType(String type) {
        if(type == null) {
            return input -> input;
        }

        switch(type) {
            case "upper":
                return input -> input == null ? null : input.toUpperCase();
            case "lower":
                return input -> input == null ? null : input.toLowerCase();
            case "camel":
                return input -> CaseUtils.toCamelCase(input, false, delimiters);
            default:
                return input -> input;
        }
    }

}
