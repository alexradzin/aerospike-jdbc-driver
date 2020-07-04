package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.IOUtils;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toCollection;

public class ExpressionAwareResultSetFactory {
    private final Collection<String> functionNames;
    private final DriverPolicy driverPolicy;

    private static final Pattern EXPRESSION_DELIMITERS = Pattern.compile("[\\s()[],;.*+-/=><?:%^&!]]");
    private static final Pattern NUMBER = Pattern.compile("\\b[+-]?\\d+(:?\\.\\d+)?(:?e[+-]?\\d+)?\\b");
    private static final Pattern QUOTES = Pattern.compile("'[^']*'");

    public ExpressionAwareResultSetFactory(FunctionManager functionManager, DriverPolicy driverPolicy) {
        functionNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        functionNames.addAll(functionManager.getFunctionNames());
        this.driverPolicy = driverPolicy;
    }

    public ResultSet wrap(ResultSet rs, FunctionManager functionManager, List<DataColumn> columns, boolean indexByName) {
        return new ExpressionAwareResultSet(rs, functionManager, driverPolicy, columns, indexByName);
    }

    public Collection<String> getVariableNames(String expr) {
        return Arrays.stream(EXPRESSION_DELIMITERS.split(NUMBER.matcher(QUOTES.matcher(expr).replaceAll("")).replaceAll(" ")))
                .filter(w -> !NUMBER.matcher(w).matches())
                .filter(w -> !(w.startsWith("'") && w.endsWith("'")))
                .filter(w -> !functionNames.contains(w))
                .filter(w -> !w.isEmpty())
                .map(IOUtils::stripQuotes)
                .collect(toCollection(LinkedHashSet::new));
    }

    public Collection<String> getClientSideFunctionNames() {
        return functionNames;
    }
}
