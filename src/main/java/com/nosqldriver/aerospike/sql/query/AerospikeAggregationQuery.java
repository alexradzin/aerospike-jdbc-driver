package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeResultSet;

import java.sql.ResultSet;

public class AerospikeAggregationQuery extends AerospikeQuery<Statement, QueryPolicy> {
    private final String[] aliases;

    public AerospikeAggregationQuery(String schema, String[] names, String[] aliases, Statement statement, QueryPolicy policy) {
        super(schema, names, statement, policy);
        this.aliases = aliases;
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeResultSet(schema, names, client.queryAggregate(policy, criteria));
    }
}
