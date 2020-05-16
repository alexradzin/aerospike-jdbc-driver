package com.nosqldriver.util;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.IOException;
import java.io.Reader;

public class ScriptEngineWrapper implements ScriptEngine {
    private final ScriptEngine engine;

    public ScriptEngineWrapper(String engineName) {
        this(new ScriptEngineManager().getEngineByName(engineName));
    }

    public ScriptEngineWrapper(ScriptEngine engine) {
        this.engine = engine;
    }

    @Override
    public Object eval(String script, ScriptContext context) throws ScriptException {
        return unwrapResult(engine.eval(wrapScript(script), context));
    }

    @Override
    public Object eval(Reader reader, ScriptContext context) throws ScriptException {
        return unwrapResult(engine.eval(wrapScript(read(reader)), context));
    }

    @Override
    public Object eval(String script) throws ScriptException {
        return unwrapResult(engine.eval(wrapScript(script)));
    }

    @Override
    public Object eval(Reader reader) throws ScriptException {
        return unwrapResult(engine.eval(wrapScript(read(reader))));
    }

    @Override
    public Object eval(String script, Bindings n) throws ScriptException {
        return unwrapResult(engine.eval(wrapScript(script), n));
    }

    @Override
    public Object eval(Reader reader, Bindings n) throws ScriptException {
        return unwrapResult(engine.eval(wrapScript(read(reader)), n));
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

    protected String wrapScript(String script) {
        return script;
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
