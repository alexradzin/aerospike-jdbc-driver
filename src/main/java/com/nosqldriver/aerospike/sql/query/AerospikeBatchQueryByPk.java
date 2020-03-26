package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.util.List;
import java.util.stream.IntStream;

public class AerospikeBatchQueryByPk extends AerospikeQuery<Key[], BatchPolicy, KeyRecord> {

    public AerospikeBatchQueryByPk(java.sql.Statement sqlStatement, String schema, String set, List<DataColumn> columns, Key[] keys, BatchPolicy policy, KeyRecordFetcherFactory keyRecordFetcherFactory, FunctionManager functionManager, boolean pk) {
        super(sqlStatement, schema, set, columns, keys, policy, keyRecordFetcherFactory, functionManager, pk);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(statement, schema, set, columns, zip(criteria, client.get(policy, criteria)), keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set), functionManager, pk);
    }


    private static KeyRecord[] zip(Key[] keys, Record[] records) {
        return IntStream.range(0, keys.length).mapToObj(i -> new KeyRecord(keys[i], records[i])).toArray(KeyRecord[]::new);
    }
}
