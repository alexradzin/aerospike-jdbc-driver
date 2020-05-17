package com.nosqldriver.sql;

import com.nosqldriver.sql.DriverPolicy.Script;
import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.JavaScriptEngineWrapper;
import com.nosqldriver.util.LuaScriptEngineWrapper;
import com.nosqldriver.util.ScriptEngineWrapper;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.Arrays.asList;
import static javax.script.ScriptContext.ENGINE_SCOPE;
import static javax.script.ScriptContext.GLOBAL_SCOPE;
import static javax.script.ScriptEngine.ARGV;
import static javax.script.ScriptEngine.ENGINE;
import static javax.script.ScriptEngine.ENGINE_VERSION;
import static javax.script.ScriptEngine.FILENAME;
import static javax.script.ScriptEngine.LANGUAGE;
import static javax.script.ScriptEngine.LANGUAGE_VERSION;
import static javax.script.ScriptEngine.NAME;

public class ScriptEngineFactory {
    private static final Object lock = new Object();
    private static final ThreadLocal<ScriptEngine> threadEngine = new ThreadLocal<>();
    private static final Map<Script, Supplier<ScriptEngineWrapper>> scriptEngineFactories = new LinkedHashMap<>();
    static {
        scriptEngineFactories.put(Script.js, JavaScriptEngineWrapper::new);
        scriptEngineFactories.put(Script.lua, LuaScriptEngineWrapper::new);
    }
    private static final Set<String> internalScriptConstants = new HashSet<>(asList(ARGV, ENGINE, ENGINE_VERSION, FILENAME, LANGUAGE_VERSION, LANGUAGE, NAME));
    private final ScriptEngine engine;

    public ScriptEngineFactory(FunctionManager functionManager, DriverPolicy driverPolicy) {
        this(Collections.emptyMap(), functionManager, driverPolicy);
    }

    private ScriptEngineFactory(Map<String, Object> bindings, FunctionManager functionManager, DriverPolicy driverPolicy) {
        synchronized (lock) {
            ScriptEngine tmp = threadEngine.get();
            if (tmp == null) {
                engine = driverPolicy.getScript() == null ? scriptEngineFactories.values().stream().map(Supplier::get).filter(ScriptEngineWrapper::isValid).findFirst()
                        .orElseThrow(() -> new IllegalStateException("Cannot initialize scripting engine"))
                        :
                        scriptEngineFactories.get(driverPolicy.getScript()).get();
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
        Bindings b = new SimpleBindings(new TreeMap<>(CASE_INSENSITIVE_ORDER));
        b.putAll(engine.getBindings(scope).entrySet().stream()
                .filter(e -> internalScriptConstants.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        engine.setBindings(b, scope);
    }
}
