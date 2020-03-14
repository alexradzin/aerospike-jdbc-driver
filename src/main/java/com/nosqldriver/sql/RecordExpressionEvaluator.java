package com.nosqldriver.sql;

import com.aerospike.client.Record;
import com.nosqldriver.util.FunctionManager;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class RecordExpressionEvaluator extends ExpressionEvaluator<Record> implements Predicate<Record>, Function<Record, Object> {
    public RecordExpressionEvaluator(String expr, Map<String, Object> initialBindings, FunctionManager functionManager) {
        super(expr, initialBindings, functionManager);
    }

    @Override
    protected Map<String, Object> toMap(Record record) {
        return record.bins;
    }
}
