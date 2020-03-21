package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.SneakyThrower;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;

public class ExpressionAwareResultSetFactory {
    private final List<String> functionNames = new ArrayList<>();

    private static final Pattern EXPRESSION_DELIMITERS = Pattern.compile("[\\s()[],;.*+-/=><?:%^&!]]");
    private static final Pattern NUMBER = Pattern.compile("\\b[+-]?\\d+(:?\\.\\d+)?(:?e[+-]?\\d+)?\\b");
    private static final Pattern QUOTES = Pattern.compile("'[^']*'");

    public ExpressionAwareResultSetFactory() {
        SneakyThrower.call(() -> {
            @SuppressWarnings("unchecked")
            List<Object> definedFunctions = (List<Object>) new JavascriptEngineFactory(null).getEngine().eval("functions");
            this.functionNames.addAll(definedFunctions.stream().filter(f -> f instanceof String).map(f -> (String)f).filter(f -> !f.startsWith("_")).collect(Collectors.toList()));
        });
    }

    public void addFunctionNames(Collection<String> functionNames) {
        this.functionNames.addAll(functionNames);
    }

    public ResultSet wrap(ResultSet rs, FunctionManager functionManager, List<DataColumn> columns, boolean indexByName) {
        return new ExpressionAwareResultSet(rs, functionManager, columns, indexByName);
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
