package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;

public class AerospikeBatchQueryByPk extends AerospikeQuery<Key[], BatchPolicy> {
    public AerospikeBatchQueryByPk(String schema, String[] names, Key[] keys, BatchPolicy policy) {
        super(schema, names, keys, policy);
    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, names, client.get(policy, criteria));
    }
}
