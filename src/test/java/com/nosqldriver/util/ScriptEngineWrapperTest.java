package com.nosqldriver.util;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;

import java.io.Reader;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScriptEngineWrapperTest {
    private final ScriptEngine engine = mock(ScriptEngine.class);
    private final ScriptEngineWrapper wrapped = Mockito.spy(new ScriptEngineWrapper(engine));
    private final ScriptContext context = mock(ScriptContext.class);
    private final Bindings bindings = new SimpleBindings();

    @Test
    void evalScriptAndScriptContext() throws ScriptException {
        when(engine.eval("script", context)).thenReturn("result");
        assertEquals("result", wrapped.eval("script", context));
        verify(engine, times(1)).eval("script", context);
        verify(wrapped, times(1)).unwrapResult("result");
        verify(wrapped, times(1)).wrapScript("script");
    }

    @Test
    void evalScript() throws ScriptException {
        when(engine.eval("script")).thenReturn("result");
        assertEquals("result", wrapped.eval("script"));
        verify(engine, times(1)).eval("script");
        verify(wrapped, times(1)).unwrapResult("result");
        verify(wrapped, times(1)).wrapScript("script");
    }

    @Test
    void evalReaderAndScriptContext() throws ScriptException {
        Reader reader = new StringReader("script");
        when(engine.eval("script", context)).thenReturn("result");
        assertEquals("result", wrapped.eval(reader, context));
        verify(engine, times(1)).eval("script", context);
        verify(wrapped, times(1)).unwrapResult("result");
        verify(wrapped, times(1)).wrapScript("script");
    }

    @Test
    void evalReader() throws ScriptException {
        Reader reader = new StringReader("script");
        when(engine.eval("script")).thenReturn("result");
        assertEquals("result", wrapped.eval(reader));
        verify(engine, times(1)).eval("script");
        verify(wrapped, times(1)).unwrapResult("result");
        verify(wrapped, times(1)).wrapScript("script");
    }

    @Test
    void evalScriptAndBindings() throws ScriptException {
        when(engine.eval("script", bindings)).thenReturn("result");
        assertEquals("result", wrapped.eval("script", bindings));
        verify(engine, times(1)).eval("script", bindings);
        verify(wrapped, times(1)).unwrapResult("result");
        verify(wrapped, times(1)).wrapScript("script");
    }

    @Test
    void evalReaderAndBindings() throws ScriptException {
        Reader reader = new StringReader("script");
        when(engine.eval("script", bindings)).thenReturn("result");
        assertEquals("result", wrapped.eval(reader, bindings));
        verify(engine, times(1)).eval("script", bindings);
        verify(wrapped, times(1)).unwrapResult("result");
        verify(wrapped, times(1)).wrapScript("script");
    }

    @Test
    void get() {
        when(engine.get("hello")).thenReturn("bye");
        assertEquals("bye", wrapped.get("hello"));
        verify(engine, times(1)).get("hello");
    }

    @Test
    void put() {
        doNothing().when(engine).put("hello", "bye");
        wrapped.put("hello", "bye");
        verify(engine, times(1)).put("hello", "bye");
    }

    @Test
    void getBindings() {
        when(engine.getBindings(123)).thenReturn(bindings);
        assertEquals(bindings, wrapped.getBindings(123));
        verify(engine, times(1)).getBindings(123);
    }

    @Test
    void setBindings() {
        doNothing().when(engine).setBindings(bindings, 321);
        wrapped.setBindings(bindings, 321);
        verify(engine, times(1)).setBindings(bindings, 321);
    }

    @Test
    void createBindings() {
        when(engine.createBindings()).thenReturn(bindings);
        assertEquals(bindings, wrapped.createBindings());
        verify(engine, times(1)).createBindings();
    }

    @Test
    void getContext() {
        when(engine.getContext()).thenReturn(context);
        assertEquals(context, wrapped.getContext());
        verify(engine, times(1)).getContext();
    }

    @Test
    void setContext() {
        doNothing().when(engine).setContext(context);
        wrapped.setContext(context);
        verify(engine, times(1)).setContext(context);
    }

    @Test
    void getFactory() {
        ScriptEngineFactory factory = mock(ScriptEngineFactory.class);
        when(engine.getFactory()).thenReturn(factory);
        assertEquals(factory, wrapped.getFactory());
        verify(engine, times(1)).getFactory();
    }
}
