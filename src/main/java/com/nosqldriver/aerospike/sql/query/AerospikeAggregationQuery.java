package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecordSet;
import com.nosqldriver.sql.ResultSetOverIterableFactory;

public class AerospikeAggregationQuery extends AerospikeQuery<Statement, QueryPolicy> {
    private final String[] aliases;

    public AerospikeAggregationQuery(String schema, String[] names, String[] aliases, Statement statement, QueryPolicy policy) {
        super(schema, names, statement, policy);
        this.aliases = aliases;
    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverIterableFactory().create(schema, names, aliases, client.queryAggregate(policy, criteria));
    }
}
