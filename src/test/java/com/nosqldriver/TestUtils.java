package com.nosqldriver;

import org.junit.jupiter.api.DisplayName;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

public class TestUtils {
    public static String getDisplayName() {
        return Arrays.stream(new Throwable().getStackTrace())
                .map(s -> getMethod(getClass(s.getClassName()), s.getMethodName()))
                .map(m -> m.getAnnotation(DisplayName.class))
                .filter(Objects::nonNull)
                .map(DisplayName::value)
                .findFirst()
                .orElse(null);
    }

    private static Class getClass(String name) {
        try {
            return Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(name, e);
        }
    }


    private static Method getMethod(Class<?> clazz, String name, Class<?> ... params) {
        try {
            return clazz.getDeclaredMethod(name, params);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(String.format("%s.%s(%s)", clazz, name, Arrays.toString(params)), e);
        }
    }
}
