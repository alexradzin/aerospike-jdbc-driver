package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeResultSet;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeAggregationQuery extends AerospikeQuery<Statement, QueryPolicy> {
    private final String set;
    public AerospikeAggregationQuery(String schema, String set, String[] names, List<DataColumn> columns, Statement statement, QueryPolicy policy) {
        super(schema, names, columns, statement, policy);
        this.set = set;
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeResultSet(schema, set, columns, client.queryAggregate(policy, criteria));
    }
}
