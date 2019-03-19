package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;

import java.sql.ResultSet;
import java.util.function.Function;

abstract class AerospikeQuery<C, P extends Policy> implements Function<IAerospikeClient, ResultSet> {
    protected final String schema;
    protected final String[] names;
    protected final C criteria;
    protected final P policy;

    protected AerospikeQuery(String schema, String[] names, C criteria, P policy) {
        this.schema = schema;
        this.names = names;
        this.criteria = criteria;
        this.policy = policy;
    }

    @Override
    public abstract ResultSet apply(IAerospikeClient client);
}
