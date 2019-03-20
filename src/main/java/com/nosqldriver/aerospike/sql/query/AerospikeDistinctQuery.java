package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.sql.ResultSetOverDistinctMapFactory;

public class AerospikeDistinctQuery extends AerospikeQuery<Statement, QueryPolicy> {
    private final String[] aliases;

    public AerospikeDistinctQuery(String schema, String[] names, String[] aliases, Statement statement, QueryPolicy policy) {
        super(schema, names, statement, policy);
        this.aliases = aliases;
    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverDistinctMapFactory().create(schema, names, aliases, client.queryAggregate(policy, criteria));
    }
}
