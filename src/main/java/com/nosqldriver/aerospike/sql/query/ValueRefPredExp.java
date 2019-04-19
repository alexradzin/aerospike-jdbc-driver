package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;

public class ValueRefPredExp extends PredExp {
    private final String table;
    private final String name;

    public ValueRefPredExp(String table, String name) {
        this.table = table;
        this.name = name;
    }

    @Override
    public int estimateSize() {
        return 0;
    }

    @Override
    public int write(byte[] buf, int offset) {
        return 0;
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return name;
    }
}
