package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeResultSet;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class AerospikeAggregationQuery extends AerospikeQuery<Statement, QueryPolicy, Map<String, Object>> {
    public AerospikeAggregationQuery(String schema, String set, List<DataColumn> columns, Statement statement, QueryPolicy policy, BiFunction<IAerospikeClient, QueryPolicy, Map<String, Object>> anyRecordSupplier) {
        super(schema, set, columns, statement, policy, anyRecordSupplier);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeResultSet(schema, set, columns, client.queryAggregate(policy, criteria), () -> anyRecordSupplier.apply(client, policy));
    }
}
