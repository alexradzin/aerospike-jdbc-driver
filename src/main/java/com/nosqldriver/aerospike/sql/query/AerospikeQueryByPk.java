package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeQueryByPk extends AerospikeQuery<Key, QueryPolicy, Record> {
    public AerospikeQueryByPk(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Key key, QueryPolicy policy, KeyRecordFetcherFactory keyRecordFetcherFactory, FunctionManager functionManager, boolean pk) {
        super(sqlStatement, schema, key.setName, columns, key, policy, keyRecordFetcherFactory, functionManager, pk);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(statement, schema, set, columns, new KeyRecord[] {new KeyRecord(criteria, client.get(policy, criteria))}, keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set), functionManager, pk);
    }
}
