package com.nosqldriver.sql;

import com.aerospike.client.Record;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.function.Function;
import java.util.function.Predicate;

public class RecordExpressionEvaluator implements Predicate<Record>, Function<Record, Object> {
    private final String expr;
    private final ScriptEngine engine;

    public RecordExpressionEvaluator(String expr) {
        this.expr = expr;
        engine = new JavascriptEngineFactory().getEngine();
    }


    @Override
    public boolean test(Record record) {
        return (Boolean)eval(record, expr.replaceAll("(?<![<>])=", "=="));
    }

    @Override
    public Object apply(Record record) {
        return eval(record, expr);
    }

    private Object eval(Record record, String expr) {
        try {
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            bindings.putAll(record.bins);
            return engine.eval(expr);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(this.expr);
        }
    }
}
