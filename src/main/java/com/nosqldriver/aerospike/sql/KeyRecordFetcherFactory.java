package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.VisibleForPackage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;

public class KeyRecordFetcherFactory {
    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private static final Function<Key, Map<String, Object>> keyExtractor = key -> key != null && key.userKey != null ? Collections.singletonMap("PK", key.userKey.getObject()) : emptyMap();
    private static final Function<Key, Map<String, Object>> keyDigestExtractor = key -> key != null && key.digest != null ? Collections.singletonMap("PK_DIGEST", key.digest) : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> keyDigestRecordKeyExtractor = keyRecord -> keyRecord != null ? keyDigestExtractor.apply(keyRecord.key) : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> keyRecordKeyExtractor = keyRecord -> keyRecord != null ? map(keyRecord.key, keyExtractor) : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> emptyKeyRecordExtractor = keyRecord -> emptyMap();

    private static Map<SpecialField, Function<KeyRecord, Map<String, Object>>> fieldToExtractor = new HashMap<>();
    static {
        fieldToExtractor.put(SpecialField.PK, keyRecordKeyExtractor);
        fieldToExtractor.put(SpecialField.PK_DIGEST, keyDigestRecordKeyExtractor);
    }

    private final QueryPolicy queryPolicy;

    public KeyRecordFetcherFactory(QueryPolicy queryPolicy) {
        this.queryPolicy = queryPolicy;
    }

    public BiFunction<String, String, Iterable<KeyRecord>> createKeyRecordsFetcher(IAerospikeClient client, String catalog, String table) {
        return (s, s2) -> {
            com.aerospike.client.query.Statement statement = new com.aerospike.client.query.Statement();
            statement.setNamespace(catalog);
            statement.setSetName(table);
            return client.query(queryPolicy, statement);
        };
    }


    @SafeVarargs
    private static Map<String, Object> map(Key key, Function<Key, Map<String, Object>> ... extractors) {
        if (extractors.length == 1) {
            return extractors[0].apply(key);
        }
        Map<String, Object> map = new HashMap<>();
        Arrays.stream(extractors).forEach(e -> map.putAll(e.apply(key)));
        return map;
    }

    public static Function<KeyRecord, Map<String, Object>>[] extractors(Collection<SpecialField> specialFields) {
        List<Function<KeyRecord, Map<String, Object>>> list = fieldToExtractor.entrySet().stream()
                .filter(e -> specialFields.contains(e.getKey())).map(Map.Entry::getValue).collect(Collectors.toList());
        list.add(keyRecordDataExtractor);
        int n = list.size();
        @SuppressWarnings("unchecked")
        Function<KeyRecord, Map<String, Object>>[] array = new Function[n];
        for (int i = 0; i < n; i++) {
            array[i] = list.get(i);
        }
        return array;
    }
}
