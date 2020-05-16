package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.JavaScriptEngineWrapper;
import com.nosqldriver.util.LuaScriptEngineWrapper;
import com.nosqldriver.util.ScriptEngineWrapper;

import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static javax.script.ScriptContext.ENGINE_SCOPE;
import static javax.script.ScriptContext.GLOBAL_SCOPE;

public class ScriptEngineFactory {
    private static final ThreadLocal<ScriptEngine> threadEngine = new ThreadLocal<>();

    private static final Map<String, Supplier<ScriptEngineWrapper>> scriptEngineFactories = new LinkedHashMap<>();
    static {
        scriptEngineFactories.put("js", JavaScriptEngineWrapper::new);
        scriptEngineFactories.put("lua", LuaScriptEngineWrapper::new);
    }
    private final ScriptEngine engine;

    public ScriptEngineFactory(FunctionManager functionManager) {
        this(Collections.emptyMap(), functionManager);
    }

    private ScriptEngineFactory(Map<String, Object> bindings, FunctionManager functionManager) {
        synchronized (threadEngine) {
            ScriptEngine tmp = threadEngine.get();
            if (tmp == null) {
                engine = scriptEngineFactories.values().stream().map(Supplier::get).filter(ScriptEngineWrapper::isValid).findFirst()
                        .orElseThrow(() -> new IllegalStateException("Cannot initialize scripting engine"));
                setBindings(GLOBAL_SCOPE);
                setBindings(ENGINE_SCOPE);
                threadEngine.set(engine);
            } else {
                engine = tmp;
                engine.getBindings(ENGINE_SCOPE).clear();
            }
            if (functionManager != null) {
                functionManager.getFunctionNames().forEach(name -> engine.put(name, functionManager.getFunction(name)));
            }
            engine.getBindings(ENGINE_SCOPE).putAll(bindings);
        }
    }

    public ScriptEngine getEngine() {
        return engine;
    }

    private void setBindings(int scope) {
        engine.setBindings(new SimpleBindings(new TreeMap<>(CASE_INSENSITIVE_ORDER)), scope);
    }
}
