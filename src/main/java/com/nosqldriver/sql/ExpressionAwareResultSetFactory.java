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
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
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


    private static final Map<Class, Integer> sqlTypes = new HashMap<>();
    static {
        sqlTypes.put(Short.class, Types.SMALLINT);
        sqlTypes.put(Integer.class, Types.INTEGER);
        sqlTypes.put(Long.class, Types.BIGINT);
        sqlTypes.put(Boolean.class, Types.BOOLEAN);
        sqlTypes.put(Float.class, Types.FLOAT);
        sqlTypes.put(Double.class, Types.DOUBLE);
        sqlTypes.put(String.class, Types.VARCHAR);
        sqlTypes.put(byte[].class, Types.BLOB);
        sqlTypes.put(Date.class, Types.DATE);
    }


    public ExpressionAwareResultSetFactory() {
        try {
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
                } else if (isGetByIndex(method)) {
                    int index = (Integer)args[0] - 1;
                    eval = evals.size() > index ? evals.get(index) : null;
                } else if (isMetadata(method)) {
                    ResultSetMetaData md = rs.getMetaData();
                    int[] discoveredExpressionTypes = new int[evals.size()];
                    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
                    Collection<String> bound = bind(rs, names, evals, bindings);
                    int n = md.getColumnCount();
                    Map<String, Integer> knownColumnTypes = new HashMap<>();
                    for (int i = 0; i < n; i++) {
                        String name = md.getColumnName(i + 1);
                        if (name != null && !bound.contains(name)) {
                            int type = md.getColumnType(i + 1);
                            knownColumnTypes.put(name, type);
                            Object value = null;
                            switch (type) {
                                case Types.BIGINT: case Types.INTEGER: case Types.SMALLINT: value = currentTimeMillis(); break;
                                case Types.DOUBLE: case Types.FLOAT: value = Math.PI * Math.E; break;
                                case Types.VARCHAR: case Types.LONGNVARCHAR: value = ""; break;
                            }
                            if (value != null) {
                                bindings.put(name, value);
                            }
                        }
                    }

                    boolean typesFound = false;
                    for (int i = 0; i < evals.size(); i++) {
                        String e = evals.get(i);
                        if (e == null) {
                            continue;
                        }
                        Object result = engine.eval(e);
                        if (result != null) {
                            Integer sqlType = sqlTypes.get(result.getClass());
                            if (sqlType != null) {
                                discoveredExpressionTypes[i] = sqlType;
                                typesFound = true;
                            }
                        }
                    }

                    if (typesFound) {
                        int[] allTypes = new int[n];
                        String[] allNames = new String[n];
                        String[] allAliases = new String[n];
                        for (int i = 0; i < n; i++) {
                            allTypes[i] = discoveredExpressionTypes[i] != 0 ? discoveredExpressionTypes[i] : md.getColumnType(i + 1);
                            allNames[i] = md.getColumnName(i + 1);
                            allAliases[i] = md.getColumnLabel(i + 1);
                        }
                        return new SimpleResultSetMetaData(null, md.getSchemaName(1), allNames, allAliases, allTypes);
                    }
                    return md;
                }

                if (eval != null) {
                    Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
                    Collection<String> bound = bind(rs, names, evals,bindings);
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

    private Collection<String> bind(ResultSet rs, Collection<String> names, List<String> evals, Bindings bindings) throws SQLException {
        Collection<String> bound = new HashSet<>();
        for (String name : names) {
            bindings.put(name, rs.getObject(name));
            bound.add(name);
        }

        ResultSetMetaData md = rs.getMetaData();
        for (int i = 0, j = 1; i < md.getColumnCount(); i++, j++) {
            String name = md.getColumnLabel(j);
            if (evals.size() > i && evals.get(i) == null && !bindings.containsKey(name)) {
                bindings.put(name, rs.getObject(j));
                bound.add(name);
            }
        }
        return bound;
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
