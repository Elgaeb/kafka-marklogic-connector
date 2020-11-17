package com.marklogic.kafka.connect.sink.util;

import java.util.HashMap;
import java.util.Map;

public class HashMapBuilder<K, V> extends HashMap<K, V> {
    public HashMapBuilder<K, V> with(K key, V value) {
        this.put(key, value);
        return this;
    }

    public HashMapBuilder<K, V> with(Map<? extends K, ? extends V> other) {
        putAll(other);
        return this;
    }
}
