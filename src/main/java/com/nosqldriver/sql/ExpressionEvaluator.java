package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.ScriptEngineWrapper;
import com.nosqldriver.util.SneakyThrower;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Optional.ofNullable;

@VisibleForPackage
public abstract class ExpressionEvaluator<T> implements Predicate<T>, Function<T, Object> {
    private final String expr;
    private final ScriptEngine engine;
    private final String fixedExpr;

    public ExpressionEvaluator(String expr, Map<String, Object> initialBindings, FunctionManager functionManager, DriverPolicy driverPolicy) {
        this.expr = expr;
        engine = new ScriptEngineFactory(functionManager, driverPolicy).getEngine();
        fixedExpr = engine instanceof ScriptEngineWrapper ? ((ScriptEngineWrapper)engine).fixWhereExpression(expr) : expr;
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.putAll(initialBindings);
    }


    @Override
    public boolean test(T record) {
        return ofNullable((Boolean)eval(record, fixedExpr)).orElse(false);
    }

    @Override
    public Object apply(T record) {
        return eval(record, expr);
    }

    private Object eval(T record, String expr) {
        try {
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            Map<String, Object> fields = toMap(record);
            Map<String, Object> ctx = new HashMap<>();
            int replacementCount = 0;
            for (Entry<String, Object> e : fields.entrySet()) {
                String key = e.getKey();
                String trimmedKey = key.trim();
                String varName = key;
                if (!trimmedKey.matches("[a-zA-Z0-9_]+") && expr.contains(trimmedKey)) { //TODO use better pattern instead of contains to be sure that subset of expression is not replaced by mistake
                    String newVarName = "var" + replacementCount;
                    expr = expr.replace(varName, newVarName);
                    varName = newVarName;
                    replacementCount++;
                }
                ctx.put(varName, e.getValue());
            }
            bindings.putAll(ctx);
            return engine.eval(expr);
        } catch (Exception e) {
            return SneakyThrower.sneakyThrow(new SQLException(e.getMessage(), e));
        }
    }

    protected abstract Map<String, Object> toMap(T record);
}
