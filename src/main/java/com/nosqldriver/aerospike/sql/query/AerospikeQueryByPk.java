package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeQueryByPk extends AerospikeQuery<Key, Policy> {
    private String set;
    public AerospikeQueryByPk(String schema, String[] names, List<DataColumn> columns, Key key, Policy policy) {
        super(schema, names, columns, key, policy);
        set = key.setName;
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, set, columns, new Record[] {client.get(policy, criteria)});
    }
}
