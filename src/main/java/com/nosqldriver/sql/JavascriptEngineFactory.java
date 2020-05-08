package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static javax.script.ScriptContext.ENGINE_SCOPE;
import static javax.script.ScriptContext.GLOBAL_SCOPE;

public class JavascriptEngineFactory {
    private static final ThreadLocal<ScriptEngine> threadEngine = new ThreadLocal<>();
    private final ScriptEngine engine;

    public JavascriptEngineFactory(FunctionManager functionManager) {
        this(Collections.emptyMap(), functionManager);
    }

    public JavascriptEngineFactory(Map<String, Object> bindings, FunctionManager functionManager) {
        synchronized (threadEngine) {
            ScriptEngine tmp = threadEngine.get();
            if (tmp == null) {
                engine = new ScriptEngineManager().getEngineByName("JavaScript");
                setBindings(GLOBAL_SCOPE);
                setBindings(ENGINE_SCOPE);
                threadEngine.set(engine);
            } else {
                engine = tmp;
                engine.getBindings(ENGINE_SCOPE).clear();
            }
            engine.getBindings(ENGINE_SCOPE).putAll(bindings);
            if (functionManager != null) {
                functionManager.getFunctionNames().forEach(name -> {
                    engine.put(name, functionManager.getFunction(name));
                });
            }
        }
    }

    public ScriptEngine getEngine() {
        return engine;
    }

    private void setBindings(int scope) {
        engine.setBindings(new SimpleBindings(new TreeMap<>(CASE_INSENSITIVE_ORDER)), scope);
    }
}
