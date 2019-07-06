package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverDistinctMap;
import com.nosqldriver.sql.DataColumn;

import java.sql.ResultSet;
import java.util.List;

public class AerospikeDistinctQuery extends AerospikeQuery<Statement, QueryPolicy> {
    private final String set;
    public AerospikeDistinctQuery(String schema, String[] names, List<DataColumn> columns, Statement statement, QueryPolicy policy) {
        super(schema, names, columns, statement, policy);
        this.set = statement.getSetName();
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverDistinctMap(schema, set, names, columns, client.queryAggregate(policy, criteria));
    }
}
