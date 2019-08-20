package com.nosqldriver.sql;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import javax.script.ScriptException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.IntStream.range;

public class ExpressionAwareResultSetFactory {
    private final List<String> functionNames = new ArrayList<>();

    private static final Pattern EXPRESSION_DELIMITERS = Pattern.compile("[\\s()[],;.*+-/=><?:%^&!]]");
    private static final Pattern NUMBER = Pattern.compile("\\b[+-]?\\d+(:?\\.\\d+)?(:?e[+-]?\\d+)?\\b");
    private static final Pattern QUOTES = Pattern.compile("'[^']*'");

    public ExpressionAwareResultSetFactory() {
        try {
            ScriptObjectMirror definedFunctions = (ScriptObjectMirror) new JavascriptEngineFactory().getEngine().eval("functions");
            range(0, definedFunctions.size()).boxed().forEach(i -> functionNames.add((String) definedFunctions.getSlot(i)));
        } catch (ScriptException e) {
            throw new IllegalStateException(e);
        }
    }


    public ResultSet wrap(ResultSet rs, List<DataColumn> columns, boolean indexByName) {
        return new ExpressionAwareResultSet(rs, columns, indexByName);
    }

    public Collection<String> getVariableNames(String expr) {
        return Arrays.stream(EXPRESSION_DELIMITERS.split(NUMBER.matcher(QUOTES.matcher(expr).replaceAll("")).replaceAll(" ")))
                .filter(w -> !NUMBER.matcher(w).matches())
                .filter(w -> !((w.startsWith("\"") && w.endsWith("\"")) || (w.startsWith("'") && w.endsWith("'"))))
                .filter(w -> !functionNames.contains(w))
                .filter(w -> !w.isEmpty())
                .collect(toCollection(LinkedHashSet::new));
    }

    public Collection<String> getClientSideFunctionNames() {
        return functionNames;
    }
}
