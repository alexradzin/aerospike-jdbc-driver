package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.util.FunctionManager;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

abstract class AerospikeQuery<C, P extends Policy, R> implements Function<IAerospikeClient, ResultSet> {
    protected static final KeyRecordFetcherFactory keyRecordFetcherFactory = new KeyRecordFetcherFactory();
    protected final Statement statement;
    protected final String schema;
    protected final String set;
    protected final List<DataColumn> columns;
    protected final C criteria;
    protected final P policy;
    protected final FunctionManager functionManager;
    protected final boolean pk;

    protected AerospikeQuery(Statement statement, String schema, String set, List<DataColumn> columns, C criteria, P policy, FunctionManager functionManager, boolean pk) {
        this.statement = statement;
        this.schema = schema;
        this.set = set;
        this.columns = Collections.unmodifiableList(columns);
        this.criteria = criteria;
        this.policy = policy;
        this.functionManager = functionManager;
        this.pk = pk;
    }

    @Override
    public abstract ResultSet apply(IAerospikeClient client);
}
