package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.aerospike.sql.ResultSetOverDistinctMap;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.FilteredResultSet;
import com.nosqldriver.util.CustomDeserializerManager;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class AerospikeDistinctQuery extends AerospikeQuery<Statement, QueryPolicy, Map<String, Object>> {
    private final Predicate<ResultSet> having;

    @VisibleForPackage
    AerospikeDistinctQuery(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Statement statement, QueryPolicy policy, CustomDeserializerManager cdm) {
        this(sqlStatement, schema, columns, statement, policy, rs -> true, cdm);
    }

    @VisibleForPackage
    AerospikeDistinctQuery(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Statement statement, QueryPolicy policy, Predicate<ResultSet> having, CustomDeserializerManager cdm) {
        super(sqlStatement, schema, statement.getSetName(), columns, statement, policy, cdm);
        this.having = having;
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new FilteredResultSet(
                new ResultSetOverDistinctMap(
                        statement,
                        schema,
                        set,
                        columns,
                        client.queryAggregate(policy, criteria),
                        keyRecordFetcherFactory.createKeyRecordsFetcher(client, schema, set),
                        customDeserializerManager),
                columns,
                having,
                false);
    }
}
