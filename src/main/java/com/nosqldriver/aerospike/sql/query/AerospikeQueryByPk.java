package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.BiFunction;

public class AerospikeQueryByPk extends AerospikeQuery<Key, Policy, Record> {
    public AerospikeQueryByPk(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Key key, Policy policy, BiFunction<IAerospikeClient, Policy, Record> anyRecordSupplier) {
        super(sqlStatement, schema, key.setName, columns, key, policy, anyRecordSupplier);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(statement, schema, set, columns, new Record[] {client.get(policy, criteria)}, () -> anyRecordSupplier.apply(client, policy), createKeyRecordsFetcher(client, schema, set));
    }
}
