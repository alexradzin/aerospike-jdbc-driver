package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.VisibleForPackage;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class KeyRecordFetcherFactory {
    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private static final Function<Key, Map<String, Object>> keyExtractor = key -> key != null && key.userKey != null ? Collections.singletonMap("PK", key.userKey.getObject()) : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> keyRecordKeyExtractor = keyRecord -> keyRecord != null ? keyExtractor.apply(keyRecord.key) : emptyMap();
    @VisibleForPackage static final Function<KeyRecord, Map<String, Object>> emptyKeyRecordExtractor = keyRecord -> emptyMap();



    public BiFunction<String, String, Iterable<KeyRecord>> createKeyRecordsFetcher(IAerospikeClient client, String catalog, String table) {
        return (s, s2) -> {
            com.aerospike.client.query.Statement statement = new com.aerospike.client.query.Statement();
            statement.setNamespace(catalog);
            statement.setSetName(table);
            return client.query(new QueryPolicy(), statement);
        };
    }
}
