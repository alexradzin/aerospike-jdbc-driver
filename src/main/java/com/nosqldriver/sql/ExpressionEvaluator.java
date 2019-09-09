package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

@VisibleForPackage
public abstract class ExpressionEvaluator<T> implements Predicate<T>, Function<T, Object> {
    private final String expr;
    private final ScriptEngine engine;

    public ExpressionEvaluator(String expr) {
        this.expr = expr;
        engine = new JavascriptEngineFactory().getEngine();
    }


    @Override
    public boolean test(T record) {
        // TODO: this replacement is pretty naive. It might corrupt strings that contain equal sign and words "and" and "or"
        return (Boolean)eval(record, expr.replaceAll("(?<![<>])=", "==").replaceAll("(?i) AND ", " && ").replaceAll("(?i) OR ", " || ").replace("<>", "!="));
    }

    @Override
    public Object apply(T record) {
        return eval(record, expr);
    }

    private Object eval(T record, String expr) {
        try {
            String trimmedExpr = expr.replace(" ", "");
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            Map<String, Object> fields = toMap(record);
            Map<String, Object> ctx = new HashMap<>();
            int replacementCount = 0;
            for (Entry<String, Object> e : fields.entrySet()) {
                String key = e.getKey();
                String trimmedKey = key.replace(" ", "");
                String varName = key;
                if (!trimmedKey.matches("[a-zA-Z0-9_]+") && trimmedExpr.contains(trimmedKey)) { //TODO use better pattern instead of contains to be sure that subset of expressin is not replaced by mistake
                    String newVarName = "var" + replacementCount;
                    trimmedExpr = trimmedExpr.replace(varName, newVarName);
                    varName = newVarName;
                    replacementCount++;
                }
                ctx.put(varName, e.getValue());
            }
            bindings.putAll(ctx);
            return engine.eval(trimmedExpr);
        } catch (ScriptException e) {
            throw new IllegalArgumentException(this.expr);
        }
    }

    protected abstract Map<String, Object> toMap(T record);
}
