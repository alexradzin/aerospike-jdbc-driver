package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;

import java.util.function.Function;

public class AerospikeBatchQueryBySecondaryIndex implements Function<IAerospikeClient, java.sql.ResultSet> {
    private final String schema;
    private final String[] names;
    private final Statement statement;
    private final QueryPolicy policy;

    public AerospikeBatchQueryBySecondaryIndex(String schema, String[] names, Statement statement) {
        this(schema, names, statement, null);
    }

    public AerospikeBatchQueryBySecondaryIndex(String schema, String[] names, Statement statement, QueryPolicy policy) {
        this.schema = schema;
        this.names = names;
        this.statement = statement;
        this.policy = policy;
    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecordSet(schema, statement, names, client.query(policy, statement));
    }
}
