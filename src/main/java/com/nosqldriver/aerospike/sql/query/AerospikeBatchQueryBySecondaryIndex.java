package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecordSet;

public class AerospikeBatchQueryBySecondaryIndex extends AerospikeQuery<Statement, QueryPolicy> {
    public AerospikeBatchQueryBySecondaryIndex(String schema, String[] names, Statement statement, QueryPolicy policy) {
        super(schema, names, statement, policy);

    }

    @Override
    public java.sql.ResultSet apply(IAerospikeClient client) {
        return new ResultSetOverAerospikeRecordSet(schema, criteria, names, client.query(policy, criteria));
    }
}
