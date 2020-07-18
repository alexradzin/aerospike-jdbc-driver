package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;
import com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeResultSet;
import com.nosqldriver.aerospike.sql.SpecialField;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.DriverPolicy;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class AerospikeAggregationQuery extends AerospikeQuery<Statement, QueryPolicy, Map<String, Object>> {
    private final DriverPolicy driverPolicy;
    public AerospikeAggregationQuery(java.sql.Statement sqlStatement, String schema, String set, List<DataColumn> columns, Statement statement, AerospikePolicyProvider policyProvider, KeyRecordFetcherFactory keyRecordFetcherFactory, FunctionManager functionManager, Collection<SpecialField> specialFields) {
        super(sqlStatement, schema, set, columns, statement, policyProvider.getQueryPolicy(), keyRecordFetcherFactory, functionManager, specialFields);
        driverPolicy = policyProvider.getDriverPolicy();
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeResultSet(
                statement,
                schema,
                set,
                columns,
                client.queryAggregate(policy, criteria),
                keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set),
                functionManager,
                driverPolicy,
                specialFields);
    }
}
