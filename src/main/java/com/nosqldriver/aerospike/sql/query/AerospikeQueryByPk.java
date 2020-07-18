package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;
import com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecords;
import com.nosqldriver.aerospike.sql.SpecialField;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.DriverPolicy;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;

public class AerospikeQueryByPk extends AerospikeQuery<Key, QueryPolicy, Record> {
    private final DriverPolicy driverPolicy;
    public AerospikeQueryByPk(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Key key, AerospikePolicyProvider policyProvider, KeyRecordFetcherFactory keyRecordFetcherFactory, FunctionManager functionManager, Collection<SpecialField> specialFields) {
        super(sqlStatement, schema, key.setName, columns, key, policyProvider.getQueryPolicy(), keyRecordFetcherFactory, functionManager, specialFields);
        driverPolicy = policyProvider.getDriverPolicy();
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecords(statement, schema, set, columns, new KeyRecord[] {new KeyRecord(criteria, client.get(policy, criteria))}, keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set), functionManager, specialFields, driverPolicy);
    }
}
