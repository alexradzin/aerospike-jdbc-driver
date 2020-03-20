package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.util.SneakyThrower;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.sql.SQLException;
import java.util.Collections;
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

    public ExpressionEvaluator(String expr) {
        this(expr, Collections.emptyMap());
    }

    public ExpressionEvaluator(String expr, Map<String, Object> initialBindings) {
        this.expr = expr;
        engine = new JavascriptEngineFactory(null).getEngine();
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        bindings.putAll(initialBindings);
        // TODO: this replacement is pretty naive. It might corrupt strings that contain equal sign and words "and" and "or"
        fixedExpr = expr.replaceAll("(?<![<>])=", "==")
                .replaceAll("(?i)(\\w+)\\s+between\\s+(\\d+)\\s+and\\s+(\\d+)", "$1>=$2 and $1<=$3")
                .replaceAll("(?i) AND ", " && ").replaceAll("(?i) OR ", " || ").replace("<>", "!=")
                .replaceAll("(?i) like\\s+'%(.*?)%'", ".match(/.*$1.*/)!=null")
                .replaceAll("(?i) like\\s+'%(.*?)'", ".match(/.*$1__ENDOFLINEINLIKEEXPRESSION__/)!=null").replace("__ENDOFLINEINLIKEEXPRESSION__", "$")
                .replaceAll("(?i) like\\s+'(.*?)%'", ".match(/^$1.*/)!=null")
                .replaceAll("(?i)like ", "==");
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
            String trimmedExpr = expr.replace(" ", "");
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            Map<String, Object> fields = toMap(record);
            Map<String, Object> ctx = new HashMap<>();
            int replacementCount = 0;
            for (Entry<String, Object> e : fields.entrySet()) {
                String key = e.getKey();
                String trimmedKey = key.replace(" ", "");
                String varName = key;
                if (!trimmedKey.matches("[a-zA-Z0-9_]+") && trimmedExpr.contains(trimmedKey)) { //TODO use better pattern instead of contains to be sure that subset of expression is not replaced by mistake
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
            return SneakyThrower.sneakyThrow(new SQLException(e.getMessage(), e));
        }
    }

    protected abstract Map<String, Object> toMap(T record);
}
