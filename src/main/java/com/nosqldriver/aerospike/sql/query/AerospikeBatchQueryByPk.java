package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;

public class AerospikeBatchQueryByPk extends AerospikeQuery<Key[], BatchPolicy> {
    public AerospikeBatchQueryByPk(String schema, String[] names, List<DataColumn> columns, Key[] keys, BatchPolicy policy) {
        super(schema, names, columns, keys, policy);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(schema, names, columns, client.get(policy, criteria));
    }
}
