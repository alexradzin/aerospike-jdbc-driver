package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Statement;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.aerospike.sql.ResultSetOverAerospikeRecordSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ResultSetWrapper;

import java.sql.ResultSet;
import java.util.List;
import java.util.function.BiFunction;

public class AerospikeBatchQueryBySecondaryIndex extends AerospikeQuery<Statement, QueryPolicy, Record> {
    @VisibleForPackage
    AerospikeBatchQueryBySecondaryIndex(java.sql.Statement sqlStatement, String schema, List<DataColumn> columns, Statement statement, QueryPolicy policy, BiFunction<IAerospikeClient, QueryPolicy, Record> anyRecordSupplier) {
        super(sqlStatement, schema, statement.getSetName(), columns, statement, policy, anyRecordSupplier);
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        if (criteria.getSetName() == null) {
            // pure calculations statement like "select 1+2"
            return new ResultSetWrapper(null, columns, false) {
                private boolean next = true;
                @Override
                public boolean next() {
                    try {
                        return next;
                    } finally {
                        next = false;
                    }
                }

                @Override
                public void close() {
                    // do nothing here
                    // This method prevents NullPointerException being thrown otherwise because wrapped result set is null
                }
            };
        }
        return new ResultSetOverAerospikeRecordSet(statement, schema, set, columns, client.query(policy, criteria), () -> anyRecordSupplier.apply(client, policy));
    }
}
