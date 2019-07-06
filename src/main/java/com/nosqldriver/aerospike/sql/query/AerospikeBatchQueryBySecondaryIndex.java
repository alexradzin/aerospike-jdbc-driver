package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecordSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ResultSetWrapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class AerospikeBatchQueryBySecondaryIndex extends AerospikeQuery<Statement, QueryPolicy> {
    public AerospikeBatchQueryBySecondaryIndex(String schema, List<DataColumn> columns, Statement statement, QueryPolicy policy) {
        super(schema, statement.getSetName(), columns, statement, policy);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        if (criteria.getSetName() == null) {
            // pure calculations statement like "select 1+2"
            return new ResultSetWrapper(null, columns) {
                private boolean next = true;
                @Override
                public boolean next() throws SQLException {
                    try {
                        return next;
                    } finally {
                        next = false;
                    }
                }
            };
        }
        return new ResultSetOverAerospikeRecordSet(schema, set, columns, client.query(policy, criteria));
    }
}
