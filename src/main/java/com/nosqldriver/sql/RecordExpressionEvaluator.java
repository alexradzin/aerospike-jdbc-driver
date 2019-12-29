package com.nosqldriver.sql;

import com.aerospike.client.Record;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class RecordExpressionEvaluator extends ExpressionEvaluator<Record> implements Predicate<Record>, Function<Record, Object> {
    public RecordExpressionEvaluator(String expr, Map<String, Object> initialBindings) {
        super(expr, initialBindings);
    }

    public RecordExpressionEvaluator(String expr) {
        super(expr);
    }

    @Override
    protected Map<String, Object> toMap(Record record) {
        return record.bins;
    }
}
