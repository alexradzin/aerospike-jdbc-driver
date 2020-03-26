package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.KeyRecord;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

class CompositeKeyRecordExtractor implements Function<KeyRecord, Map<String, Object>> {
    private final Function<KeyRecord, Map<String, Object>>[] extractors;

    @SafeVarargs
    CompositeKeyRecordExtractor(Function<KeyRecord, Map<String, Object>>... extractors) {
        this.extractors = extractors;
    }

    @Override
    public Map<String, Object> apply(KeyRecord record) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Function<KeyRecord, Map<String, Object>> extractor : extractors) {
            result.putAll(extractor.apply(record));
        }
        return result;
    }
}
