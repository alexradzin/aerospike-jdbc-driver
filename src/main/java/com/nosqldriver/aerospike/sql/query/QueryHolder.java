package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;
import com.nosqldriver.sql.ExpressionAwareResultSetFactory;
import com.nosqldriver.sql.FilteredResultSet;
import com.nosqldriver.sql.OffsetLimit;
import com.nosqldriver.sql.ResultSetWrapper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import static java.lang.String.join;

public class QueryHolder {
    private String schema;
    private final Collection<String> indexes;
    private final AerospikePolicyProvider policyProvider;
    private String set;
    private List<String> names = new ArrayList<>();
    private List<String> aliases = new ArrayList<>();
    private List<String> expressions = new ArrayList<>();
    private Collection<String> hiddenNames = new HashSet<>();

    private final Statement statement;
    private AerospikeBatchQueryBySecondaryIndex secondayIndexQuery = null;
    private AerospikeQueryByPk pkQuery = null;
    private AerospikeBatchQueryByPk pkBatchQuery = null;
    private Filter filter;
    private List<PredExp> predExps = new ArrayList<>();
    private long offset = -1;
    private long limit = -1;

    private final ExpressionAwareResultSetFactory expressionResultSetWrappingFactory = new ExpressionAwareResultSetFactory();


    public QueryHolder(String schema, Collection<String> indexes, AerospikePolicyProvider policyProvider) {
        this.schema = schema;
        this.indexes = indexes;
        this.policyProvider = policyProvider;
         statement = new Statement();
         if (schema != null) {
             statement.setNamespace(schema);
         }
    }

    public Function<IAerospikeClient, ResultSet> getQuery() {
        if (pkQuery != null) {
            assertNull(pkBatchQuery, secondayIndexQuery);
            return wrap(pkQuery);
        }
        if (pkBatchQuery != null) {
            assertNull(pkQuery, secondayIndexQuery);
            return wrap(pkBatchQuery);
        }
        if (secondayIndexQuery != null) {
            assertNull(pkQuery, pkBatchQuery);
            return wrap(secondayIndexQuery);
        }

        return wrap(createSecondaryIndexQuery());
        //return new AerospikeBatchQueryBySecondaryIndex(schema, getNames(), statement, policyProvider.getQueryPolicy());
        //throw new IllegalStateException("Query was not created"); //TODO: pass SQL here to attach it to the exception
    }

    @SafeVarargs
    private final void assertNull(Function<IAerospikeClient, ResultSet>... queries) {
        if (Arrays.stream(queries).anyMatch(Objects::nonNull)) {
            throw new IllegalStateException("More than one queires have been created");
        }
    }

    private String[] getNames(boolean hidden) {
        List<String> all = new ArrayList<>(names);
        if (hidden) {
            all.addAll(hiddenNames);
        }
        return all.toArray(new String[0]);
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

    public void addColumn(String column, String alias, boolean expr) {
        if (expr) {
            names.add(null);
            expressions.add(column);
            aliases.add(alias);
            hiddenNames.addAll(expressionResultSetWrappingFactory.getVariableNames(column));
        } else {
            names.add(column);
            expressions.add(null);
            aliases.add(alias);
        }
        statement.setBinNames(getNames(true));
    }



    private Function<IAerospikeClient, java.sql.ResultSet>  createSecondaryIndexQuery() {
        return createSecondaryIndexQuery(filter, predExps);
    }


    private Function<IAerospikeClient, java.sql.ResultSet>  createSecondaryIndexQuery(Filter filter, List<PredExp> predExps) {
        statement.setFilter(filter);
        if (predExps.size() >= 3) {
            statement.setPredExp(predExps.toArray(new PredExp[0]));
        }
        return secondayIndexQuery = new AerospikeBatchQueryBySecondaryIndex(schema, getNames(false), statement, policyProvider.getQueryPolicy());
    }

    void createPkQuery(Key key) {
        pkQuery = new AerospikeQueryByPk(schema, getNames(false), key, policyProvider.getPolicy());
    }

    void createPkBatchQuery(Key ... keys) {
        pkBatchQuery = new AerospikeBatchQueryByPk(schema, getNames(false), keys, policyProvider.getBatchPolicy());
    }

    public String getSetName() {
        return set;
    }

    public void addPredExp(PredExp predExp) {
        predExps.add(predExp);
    }

    public void setFilter(Filter filter, String binName) {
        if (indexes.contains(join(".", schema, set, binName))) {
            this.filter = filter;
        }
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }


    private Function<IAerospikeClient, java.sql.ResultSet> wrap(Function<IAerospikeClient, java.sql.ResultSet> nakedQuery) {
        Function<IAerospikeClient, java.sql.ResultSet> wrapped = client -> expressionResultSetWrappingFactory.wrap(new ResultSetWrapper(nakedQuery.apply(client), names, aliases), hiddenNames, expressions, aliases);
        return offset >= 0 || limit >= 0 ?
                client -> new FilteredResultSet(wrapped.apply(client), new OffsetLimit(offset < 0 ? 0 : offset, limit < 0 ? Long.MAX_VALUE : limit)) :
                wrapped;
    }
}
