package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;

public class OperatorRefPredExp extends PredExp {
    private final String op;

    public OperatorRefPredExp(String op) {
        this.op = op;
    }

    @Override
    public int estimateSize() {
        return 0;
    }

    @Override
    public int write(byte[] buf, int offset) {
        return 0;
    }

    public String getOp() {
        return op;
    }
}
