package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.aerospike.sql.ResultSetOverDistinctMap;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.FilteredResultSet;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Predicate;

public class AerospikeDistinctQuery extends AerospikeQuery<Statement, QueryPolicy, Map<String, Object>> {
    private final Predicate<ResultSet> having;

    @VisibleForPackage
    AerospikeDistinctQuery(String schema, List<DataColumn> columns, Statement statement, QueryPolicy policy, BiFunction<IAerospikeClient, QueryPolicy, Map<String, Object>> anyRecordSupplier) {
        this(schema, columns, statement, policy, rs -> true, anyRecordSupplier);
    }

    @VisibleForPackage
    AerospikeDistinctQuery(String schema, List<DataColumn> columns, Statement statement, QueryPolicy policy, Predicate<ResultSet> having, BiFunction<IAerospikeClient, QueryPolicy, Map<String, Object>> anyRecordSupplier) {
        super(schema, statement.getSetName(), columns, statement, policy, anyRecordSupplier);
        this.having = having;
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new FilteredResultSet(new ResultSetOverDistinctMap(schema, set, columns, client.queryAggregate(policy, criteria), () -> anyRecordSupplier.apply(client, policy)), columns, having, false);
    }
}
