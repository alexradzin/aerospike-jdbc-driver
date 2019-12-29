package com.nosqldriver.sql;

import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class JavascriptEngineFactory {
    private static final Pattern FUNCTION_HEADER = Pattern.compile("function\\s+(\\w+)\\s*\\(");
    private static final ThreadLocal<ScriptEngine> threadEngine = new ThreadLocal<>();
    private final ScriptEngine engine;

    public JavascriptEngineFactory() {
        try {
            synchronized (threadEngine) {
                ScriptEngine tmp = threadEngine.get();
                if (tmp == null) {
                    engine = new ScriptEngineManager().getEngineByName("JavaScript");
                    threadEngine.set(engine);
                } else {
                    engine = tmp;
                    engine.getBindings(ScriptContext.ENGINE_SCOPE).clear();
                }
            }
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

    public ScriptEngine getEngine() {
        return engine;
    }
}
