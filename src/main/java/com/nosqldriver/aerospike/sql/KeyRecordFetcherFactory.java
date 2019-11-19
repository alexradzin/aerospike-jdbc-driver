package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;

import java.util.function.BiFunction;

public class KeyRecordFetcherFactory {
    public BiFunction<String, String, Iterable<KeyRecord>> createKeyRecordsFetcher(IAerospikeClient client, String catalog, String table) {
        return (s, s2) -> {
            com.aerospike.client.query.Statement statement = new com.aerospike.client.query.Statement();
            statement.setNamespace(catalog);
            statement.setSetName(table);
            return client.query(new QueryPolicy(), statement);
        };
    }
}
