package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeBatchQueryByPk extends AerospikeQuery<Key[], BatchPolicy, Record> {
    public AerospikeBatchQueryByPk(java.sql.Statement sqlStatement, String schema, String set, List<DataColumn> columns, Key[] keys, BatchPolicy policy) {
        super(sqlStatement, schema, set, columns, keys, policy);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(statement, schema, set, columns, client.get(policy, criteria), keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set));
    }
}
