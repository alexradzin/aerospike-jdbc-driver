package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ExpressionAwareResultSetFactoryTest {
    private final ExpressionAwareResultSetFactory exprFactory = new ExpressionAwareResultSetFactory(new FunctionManager(null), new DriverPolicy());

    @Test
    void emptyExpression() {
        assertVariableNames("", emptyList());
    }

    @Test
    void intNumberConstant() {
        assertVariableNames("1", emptyList());
        assertVariableNames("1234", emptyList());
    }

    @Test
    void floatNumber() {
        assertVariableNames("3.14", emptyList());
    }

    @Test
    void positiveFloatNumber() {
        assertVariableNames("+3.14", emptyList());
    }

    @Test
    void negativeFloatNumber() {
        assertVariableNames("+3.14", emptyList());
    }

    @Test
    void numberWithExponent() {
        assertVariableNames(asList("+6e23", "+6e-23", "+678e+42", "6.022e+23"), emptyList());
    }

    @Test
    void variableOnly() {
        assertVariableNames("x", singletonList("x"));
        assertVariableNames("xyz", singletonList("xyz"));
        assertVariableNames("a1", singletonList("a1"));
    }

    @Test
    void numericExpression() {
        assertVariableNames(asList("1+2", "3 + 4", "(1+2) / 3"), emptyList());
    }

    @Test
    void numericExpressionWithVariable() {
        assertVariableNames(asList("x+1", "x * 2"), singletonList("x"));
        assertVariableNames(asList("factor *= 5", "4 * ( factor + 2)"), singletonList("factor"));
    }


    @Test
    void numericExpressionWithSevaralVariables() {
        assertVariableNames(asList("x+y", "x * y"), asList("x", "y"));
    }

    @Test
    void numericExpressionWithRepatedVariables() {
        assertVariableNames(asList("x+x", "x * x"), singletonList("x"));
    }

    @Test
    void functionCall() {
        assertVariableNames(asList("len(x)", "LEN(x)", "epoch('1969-07-21 02:56:00', x)"), singletonList("x"));
    }

    @Test
    void functionCallWithConstant() {
        assertVariableNames(asList(
                "len('x')",
                "concat('x', 'y')",
                "len('a b')",
                "epoch('1969-07-21 02:56:00')",
                "epoch('1969-07-21 02:56:00 UTC', 'yyyy-MM-dd HH:mm:ss z')"
                ),
                emptyList());
    }

    private void assertVariableNames(Collection<String> exprs, List<String> expectedVariables) {
        exprs.forEach(expr -> assertVariableNames(expr, expectedVariables));
    }


    private void assertVariableNames(String expr, List<String> expectedVariables) {
        assertEquals(expectedVariables, new ArrayList<>((exprFactory.getVariableNames(expr))));
    }

}