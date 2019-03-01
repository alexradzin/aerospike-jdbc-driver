package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;

import java.util.function.Function;

public class AerospikeQueryByPk implements Function<IAerospikeClient, java.sql.ResultSet> {
    private final String schema;
    private final String[] names;
    private final Key key;
    private final Policy policy;

    public AerospikeQueryByPk(String schema, String[] names, Key key) {
        this(schema, names, key, null);
    }

    public AerospikeQueryByPk(String schema, String[] names, Key key, Policy policy) {
        this.schema = schema;
        this.names = names;
        this.key = key;
        this.policy = policy;
    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, names, new Record[] {client.get(policy, key)});
    }
}
