package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ExpressionAwarePropertyGetterTest {
    @Test
    void noProperty() {
        assertExpression(emptyMap(), "something", null);
    }

    @Test
    void simpleProperty() {
        assertExpression(singletonMap("something", "nothing"), "something", "nothing");
    }

    @Test
    void simpleNullProperty() {
        assertExpression(singletonMap("something", null), "something", null);
    }

    @Test
    void expressionNullProperty() {
        assertExpression(singletonMap("null()", null), "null()", null);
    }

    @Test
    void validMathExpressionWithoutVariables() {
        assertExpression(emptyMap(), "3 + 5", 8);
    }

    @Test
    void validMathExpressionWithVariableInMap() {
        assertExpression(singletonMap("five", 5), "five - 2", 3);
    }

    @Test
    void validMathExpressionWithVariableInPojo() {
        double actual = (Double)new ExpressionAwarePropertyGetter<Holder>(
                (holder, s) -> {
                    try {
                        return Holder.class.getDeclaredField(s).get(holder);
                    } catch (ReflectiveOperationException e) {
                        return null;
                    }
                },
                holder -> Arrays.stream(Holder.class.getDeclaredFields()).map(Field::getName).collect(toList()), new FunctionManager()).apply(new Holder(), "pi * e");

        assertEquals(Math.PI * Math.E, actual, 0.001);
    }


    private void assertExpression(Map<String, Object> props, String name, Object expectedResult) {
        assertEquals(expectedResult, new ExpressionAwarePropertyGetter<Map<String, Object>>(Map::get, Map::keySet, new FunctionManager()).apply(props, name));
    }


    private static class Holder {
        public final double pi = Math.PI;
        public final double e = Math.E;

    }

}