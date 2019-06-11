package com.nosqldriver.sql;

import com.aerospike.client.Record;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class RecordExpressionEvaluator implements Predicate<Record>, Function<Record, Object> {
    private final String expr;
    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final ScriptEngine engine = manager.getEngineByName("JavaScript");
    private static final Pattern FUNCTION_HEADER = Pattern.compile("function\\s+(\\w+)\\s*\\(");

    public RecordExpressionEvaluator(String expr) {
        this.expr = expr;

        try {
            Reader functions = new InputStreamReader(getClass().getResourceAsStream("/functions.js"));
            String allFunctionsSrc = new BufferedReader(functions).lines().collect(Collectors.joining("\n"));
            Matcher matcher = FUNCTION_HEADER.matcher(allFunctionsSrc);
            StringBuffer buffer = new StringBuffer();
            while (matcher.find()) {
                String functionName = matcher.group(1);
                String capitalizedFunctionName = functionName.toUpperCase();
                matcher.appendReplacement(buffer, format("function %s(", capitalizedFunctionName));
            }
            matcher.appendTail(buffer);
            String capitalizedFunctions = buffer.toString();

            engine.eval(allFunctionsSrc + "\n" + capitalizedFunctions);
            engine.eval(new InputStreamReader(getClass().getResourceAsStream("/functionsExposer.js")));
        } catch (ScriptException e) {
            throw new IllegalStateException(e);
        }
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
