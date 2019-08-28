package com.nosqldriver.aerospike.sql.query;

public class OperatorRefPredExp extends FakePredExp {
    private final String op;

    public OperatorRefPredExp(String op) {
        this.op = op;
    }

    public String getOp() {
        return op;
    }
}
