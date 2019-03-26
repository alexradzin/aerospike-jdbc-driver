package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;

import java.sql.ResultSet;

public class AerospikeQueryByPk extends AerospikeQuery<Key, Policy> {
    public AerospikeQueryByPk(String schema, String[] names, Key key, Policy policy) {
        super(schema, names, key, policy);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, names, new Record[] {client.get(policy, criteria)});
    }
}
