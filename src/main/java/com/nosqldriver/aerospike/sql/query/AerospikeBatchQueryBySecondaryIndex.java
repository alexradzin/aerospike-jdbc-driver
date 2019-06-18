package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecordSet;
import com.nosqldriver.sql.ResultSetInvocationHandler;
import com.nosqldriver.sql.ResultSetWrapperFactory;

import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nosqldriver.sql.ResultSetInvocationHandler.METADATA;
import static com.nosqldriver.sql.ResultSetInvocationHandler.NEXT;

public class AerospikeBatchQueryBySecondaryIndex extends AerospikeQuery<Statement, QueryPolicy> {
    public AerospikeBatchQueryBySecondaryIndex(String schema, String[] names, Statement statement, QueryPolicy policy) {
        super(schema, names, statement, policy);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        if (criteria.getSetName() == null) {
            return new ResultSetWrapperFactory().create(new ResultSetInvocationHandler<ResultSet>(NEXT | METADATA, null, schema, names, null) {
                private boolean next = true;
                @Override
                protected boolean next() throws SQLException {
                    try {
                        return next;
                    } finally {
                        next = false;
                    }
                }
            });
        }
        return new ResultSetOverAerospikeRecordSet(schema, names, client.query(policy, criteria));
    }
}
