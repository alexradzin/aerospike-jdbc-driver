package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeQueryByPk extends AerospikeQuery<Key, Policy, Record> {
    public AerospikeQueryByPk(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Key key, Policy policy, FunctionManager functionManager) {
        super(sqlStatement, schema, key.setName, columns, key, policy, functionManager);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(statement, schema, set, columns, new Record[] {client.get(policy, criteria)}, keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set), functionManager);
    }
}
