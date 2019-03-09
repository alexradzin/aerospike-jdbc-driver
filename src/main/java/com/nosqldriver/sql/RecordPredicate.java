package com.nosqldriver.sql;

import com.aerospike.client.Record;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.function.Predicate;

public class RecordPredicate implements Predicate<Record> {
    private final String whereExpr;
    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final ScriptEngine engine = manager.getEngineByName("JavaScript");

    public RecordPredicate(String whereExpr) {
        this.whereExpr = whereExpr;
    }


    @Override
    public boolean test(Record record) {
        try {
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            bindings.putAll(record.bins);
            String fixedExpression = whereExpr.replaceAll("(?<![<>])=", "==");
            return (Boolean) engine.eval(fixedExpression);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(whereExpr);
        }
    }
}
