package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ExpressionAwarePropertyGetterTest {
    @Test
    void noProperty() {
        assertExpression(Collections.emptyMap(), "something", null);
    }

    @Test
    void simpleProperty() {
        assertExpression(singletonMap("something", "nothing"), "something", "nothing");
    }

    @Test
    void validMathExpressionWithoutVariables() {
        assertExpression(Collections.emptyMap(), "3 + 5", 8);
    }

    @Test
    void validMathExpressionWithVariableInMap() {
        assertExpression(singletonMap("five", 5), "five - 2", 3.0);
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
                holder -> Arrays.stream(Holder.class.getDeclaredFields()).map(Field::getName).collect(Collectors.toList())).apply(new Holder(), "pi * e");

        assertEquals(Math.PI * Math.E, actual, 0.001);
    }


    private void assertExpression(Map<String, Object> props, String name, Object expectedResult) {
        assertEquals(expectedResult, new ExpressionAwarePropertyGetter<Map<String, Object>>(Map::get, Map::keySet).apply(props, name));
    }


    private static class Holder {
        public final double pi = Math.PI;
        public final double e = Math.E;

    }

}