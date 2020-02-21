package com.nosqldriver.aerospike.sql.query;

public class InnerQueryPredExp extends PredExpValuePlaceholder {
    private final QueryHolder holder;

    public InnerQueryPredExp(int index, QueryHolder holder) {
        super(index);
        this.holder = holder;
    }

    public QueryHolder getHolder() {
        return holder;
    }
}
