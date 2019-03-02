package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class QueryHolder {
    private String schema;
    private final AerospikePolicyProvider policyProvider;
    private String set;
    private List<String> names = new ArrayList<>();

    private final Statement statement;
    private AerospikeBatchQueryBySecondaryIndex secondayIndexQuery = null;
    private AerospikeQueryByPk pkQuery = null;
    private AerospikeBatchQueryByPk pkBatchQuery = null;



    public QueryHolder(String schema, AerospikePolicyProvider policyProvider) {
        this.schema = schema;
        this.policyProvider = policyProvider;
         statement = new Statement();
         if (schema != null) {
             statement.setNamespace(schema);
         }
    }

    public Function<IAerospikeClient, ResultSet> getQuery() {
        if (pkQuery != null) {
            assertNull(pkBatchQuery, secondayIndexQuery);
            return pkQuery;
        }
        if (pkBatchQuery != null) {
            assertNull(pkQuery, secondayIndexQuery);
            return pkBatchQuery;
        }
        if (secondayIndexQuery != null) {
            assertNull(pkQuery, pkBatchQuery);
            return secondayIndexQuery;
        }
        return new AerospikeBatchQueryBySecondaryIndex(schema, getNames(), statement, policyProvider.getQueryPolicy());
        //throw new IllegalStateException("Query was not created"); //TODO: pass SQL here to attach it to the exception
    }

    @SafeVarargs
    private final void assertNull(Function<IAerospikeClient, ResultSet>... queries) {
        if (Arrays.stream(queries).anyMatch(Objects::nonNull)) {
            throw new IllegalStateException("More than one queires have been created");
        }
    }

    private String[] getNames() {
        return names.toArray(new String[0]);
    }

    public void setSchema(String schema) {
        this.schema = schema;
        statement.setNamespace(schema);
    }

    public String getSchema() {
        return schema;
    }

    public void setSetName(String set) {
        this.set = set;
        statement.setSetName(set);
    }

    public void addBinName(String name) {
        names.add(name);
        statement.setBinNames(getNames());
    }

    public void createSecondaryIndexQuery(Filter filter) {
        statement.setFilter(filter);
        secondayIndexQuery = new AerospikeBatchQueryBySecondaryIndex(schema, getNames(), statement, policyProvider.getQueryPolicy());
    }

    public void createPkQuery(Key key) {
        pkQuery = new AerospikeQueryByPk(schema, getNames(), key, policyProvider.getPolicy());
    }

    public void createPkBatchQuery(Key ... keys) {
        pkBatchQuery = new AerospikeBatchQueryByPk(schema, getNames(), keys, policyProvider.getBatchPolicy());
    }

    public String getSetName() {
        return set;
    }
}
