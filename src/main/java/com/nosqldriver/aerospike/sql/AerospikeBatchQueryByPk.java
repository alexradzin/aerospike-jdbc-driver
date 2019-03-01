package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;

import java.util.function.Function;

public class AerospikeBatchQueryByPk implements Function<IAerospikeClient, java.sql.ResultSet> {
    private final String schema;
    private final String[] names;
    private final Key[] keys;
    private final BatchPolicy policy;

    public AerospikeBatchQueryByPk(String schema, String[] names, Key[] keys) {
        this(schema, names, keys, null);
    }

    public AerospikeBatchQueryByPk(String schema, String[] names, Key[] keys, BatchPolicy policy) {
        this.schema = schema;
        this.names = names;
        this.keys = keys;
        this.policy = policy;
    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, names, client.get(policy, keys));
    }
}
