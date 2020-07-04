package com.nosqldriver.util;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.Reader;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ScriptEngineWrapper implements ScriptEngine {
    public static final String EMPTY_COLUMN_PLACEHOLDER = "__EMPTY_COLUMN__";
    private final ScriptEngine engine;

    public ScriptEngineWrapper(String engineName) {
        this(new ScriptEngineManager().getEngineByName(engineName));
    }

    public ScriptEngineWrapper(ScriptEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return unwrapResult(engine.eval(fixScript(script), context));
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        return unwrapResult(engine.eval(fixScript(read(reader)), context));
    }

    @Override
    public Object eval(String script) throws ScriptException {
        return unwrapResult(engine.eval(fixScript(script)));
    }

    @Override
    public Object eval(Reader reader) throws ScriptException {
        return unwrapResult(engine.eval(fixScript(read(reader))));
    }

    @Override
    public Object eval(String script, Bindings n) throws ScriptException {
        return unwrapResult(engine.eval(fixScript(script), n));
    }

    @Override
    public Object eval(Reader reader, Bindings n) throws ScriptException {
        return unwrapResult(engine.eval(fixScript(read(reader)), n));
    }

    @Override
    public void put(String key, Object value) {
        engine.put(key, value);
    }

    @Override
    public Object get(String key) {
        return engine.get(key);
    }

    @Override
    public Bindings getBindings(int scope) {
        return engine.getBindings(scope);
    }

    @Override
    public void setBindings(Bindings bindings, int scope) {
        engine.setBindings(bindings, scope);
    }

    @Override
    public Bindings createBindings() {
        return engine.createBindings();
    }

    @Override
    public ScriptContext getContext() {
        return engine.getContext();
    }

    @Override
    public void setContext(ScriptContext context) {
        engine.setContext(context);
    }

    @Override
    public ScriptEngineFactory getFactory() {
        return engine.getFactory();
    }

    protected String fixScript(String script) {
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        if (bindings == null) {
            return script;
        }
        AtomicReference<String> fixedScript = new AtomicReference<>(script);
        bindings.entrySet().stream().filter(e -> !isFunction(e.getValue())).map(Map.Entry::getKey)
                .forEach(name -> fixedScript.getAndUpdate(s -> s == null ? null : s.replace("\"" + name + "\"", name)));

        return fixedScript.get().replace("\"\"", EMPTY_COLUMN_PLACEHOLDER);
    }

    private boolean isFunction(Object obj) {
        return obj instanceof Function || obj instanceof BiFunction || obj instanceof Predicate || obj instanceof Supplier || obj instanceof TriFunction || obj instanceof VarargsFunction;
    }

    protected Object unwrapResult(Object obj) {
        return obj;
    }

    private String read(Reader reader) throws ScriptException {
        try {
            return IOUtils.toString(reader);
        } catch (IOException e) {
            throw new ScriptException(e);
        }
    }

    public String fixWhereExpression(String expression) {
        return expression;
    }

    public boolean isValid() {
        return engine != null;
    }
}
