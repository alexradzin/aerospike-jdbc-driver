package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeBatchQueryByPk extends AerospikeQuery<Key[], BatchPolicy> {
    private final String set;
    public AerospikeBatchQueryByPk(String schema, String set, String[] names, List<DataColumn> columns, Key[] keys, BatchPolicy policy) {
        super(schema, names, columns, keys, policy);
        this.set = set;
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, set, columns, client.get(policy, criteria));
    }
}
