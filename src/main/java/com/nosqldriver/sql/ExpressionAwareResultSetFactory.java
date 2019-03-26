package com.nosqldriver.sql;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

public class ExpressionAwareResultSetFactory {
    private final ScriptEngineManager manager = new ScriptEngineManager();
    private final ScriptEngine engine = manager.getEngineByName("JavaScript");

    private final List<String> functionNames = new ArrayList<>();
    private static final Pattern EXPRESSION_DELIMITERS = Pattern.compile("[\\s()[],;.*+-/=><?:%^&!]]");
    private static final Pattern NUMBER = Pattern.compile("\\b[+-]?\\d+(:?\\.\\d+)?(:?e[+-]?\\d+)?\\b");


    private static final Pattern FUNCTION_HEADER = Pattern.compile("function\\s+(\\w+)\\s*\\(");
    private final ResultSetWrapperFactory wrapperFactory = new ResultSetWrapperFactory();

    public ExpressionAwareResultSetFactory() {
        try {
            Reader functions = new InputStreamReader(getClass().getResourceAsStream("/functions.js"));
            String allFunctionsSrc = new BufferedReader(functions).lines().collect(Collectors.joining("\n"));
            Matcher matcher = FUNCTION_HEADER.matcher(allFunctionsSrc);
            StringBuffer buffer = new StringBuffer();
            while (matcher.find()) {
                String functionName = matcher.group(1);
                String capitalizedFunctionName = functionName.toUpperCase();
                matcher.appendReplacement(buffer, String.format("function %s(", capitalizedFunctionName));
            }
            matcher.appendTail(buffer);
            String capitalizedFunctions = buffer.toString();

            engine.eval(allFunctionsSrc + "\n" + capitalizedFunctions);
            engine.eval(new InputStreamReader(getClass().getResourceAsStream("/functionsExposer.js")));

            ScriptObjectMirror definedFunctions = (ScriptObjectMirror)engine.eval("functions");
            range(0, definedFunctions.size()).boxed().forEach(i -> functionNames.add((String)definedFunctions.getSlot(i)));
        } catch (ScriptException e) {
            throw new IllegalStateException(e);
        }
    }


    public ResultSet wrap(ResultSet rs, Collection<String> names, List<String> evals, List<String> aliases) {
        return wrapperFactory.create(new ResultSetInvocationHandler<ResultSet>(0, rs, null, names.toArray(new String[0]), aliases.toArray(new String[0])) {
            Map<String, String> aliasToEval = range(0, aliases.size()).boxed().filter(i -> evals.size() > i && evals.get(i) != null).collect(toMap(aliases::get, evals::get));

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String eval = null;
                if (isGetByName(method)) {
                    String key = (String)args[0];
                    eval = aliasToEval.get(key);
                }
                if (isGetByIndex(method)) {
                    int index = (Integer)args[0] - 1;
                    eval = evals.size() > index ? evals.get(index) : null;
                }

                if (eval != null) {
                    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
                    Collection<String> bound = new HashSet<>();
                    for (String name : names) {
                        bindings.put(name, rs.getObject(name));
                        bound.add(name);
                    }

                    ResultSetMetaData md = rs.getMetaData();
                    for (int i = 0, j = 1; i < md.getColumnCount(); i++, j++) {
                        String name = md.getColumnLabel(j);
                        if (evals.get(i) == null && !bindings.containsKey(name)) {
                            bindings.put(name, rs.getObject(j));
                            bound.add(name);
                        }
                    }

                    try {
                        return cast(engine.eval(eval), method.getReturnType());
                    } finally {
                        bound.forEach(bindings::remove);
                    }
                }
                return method.invoke(rs, args);
            }
        });
    }


    public Collection<String> getVariableNames(String expr) {
        return Arrays.stream(EXPRESSION_DELIMITERS.split(NUMBER.matcher(expr).replaceAll(" ")))
                .filter(w -> !NUMBER.matcher(w).matches())
                .filter(w -> !((w.startsWith("\"") && w.endsWith("\"")) || (w.startsWith("'") && w.endsWith("'"))))
                .filter(w -> !functionNames.contains(w))
                .filter(w -> !w.isEmpty())
                .collect(toCollection(LinkedHashSet::new));
    }

    public Collection<String> getClientSideFuctionNames() {
        return functionNames;
    }
}