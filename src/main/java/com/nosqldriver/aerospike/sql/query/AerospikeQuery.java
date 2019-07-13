package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

abstract class AerospikeQuery<C, P extends Policy, R> implements Function<IAerospikeClient, ResultSet> {
    protected final String schema;
    protected final String set;
    protected final List<DataColumn> columns;
    protected final C criteria;
    protected final P policy;
    protected final BiFunction<IAerospikeClient, P, R> anyRecordSupplier;

    protected AerospikeQuery(String schema, String set, List<DataColumn> columns, C criteria, P policy, BiFunction<IAerospikeClient, P, R> anyRecordSupplier) {
        this.schema = schema;
        this.set = set;
        this.columns = Collections.unmodifiableList(columns);
        this.criteria = criteria;
        this.policy = policy;
        this.anyRecordSupplier = anyRecordSupplier;
    }

    @Override
    public abstract ResultSet apply(IAerospikeClient client);
}
