package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;
import com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeScan;
import com.nosqldriver.aerospike.sql.SpecialField;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.DriverPolicy;
import com.nosqldriver.sql.FilteredResultSet;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

public class AerospikeScanQuery extends AerospikeQuery<Predicate<ResultSet>, ScanPolicy, Record> {
    private final DriverPolicy driverPolicy;
    public AerospikeScanQuery(java.sql.Statement sqlStatement, String schema, String set, List<DataColumn> columns, Predicate<ResultSet> predicate, AerospikePolicyProvider policyProvider, KeyRecordFetcherFactory keyRecordFetcherFactory, FunctionManager functionManager, Collection<SpecialField> specialFields) {
        super(sqlStatement, schema, set, columns, predicate, policyProvider.getScanPolicy(), keyRecordFetcherFactory, functionManager, specialFields);
        driverPolicy = policyProvider.getDriverPolicy();
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new FilteredResultSet(
                new ResultSetOverAerospikeScan(client, statement, schema, set, columns, keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set), functionManager, driverPolicy, specialFields),
                columns,
                criteria,
                true);
    }
}
