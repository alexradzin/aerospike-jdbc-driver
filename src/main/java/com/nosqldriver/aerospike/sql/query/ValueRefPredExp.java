package com.nosqldriver.aerospike.sql.query;


public class ValueRefPredExp extends FakePredExp {
    private final String table;
    private final String name;

    public ValueRefPredExp(String table, String name) {
        this.table = table;
        this.name = name;
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return name;
    }
}
