package com.nosqldriver.util;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

public class ValueExtractor {
    private static final String[] getterPrefixes = {"get", "is"};

    public Object getValue(Object obj, String key) {
        String[] path = key.replace("]", "").split("\\[");

        Object value = obj;
        for (String p : path) {
            if (value instanceof Map) {
                value = ((Map) value).get(p);
                continue;
            }
            Class clazz = value.getClass();
            Method getter = null;

            for (String prefix: getterPrefixes) {
                try {
                    //noinspection unchecked
                    getter = clazz.getMethod(prefix + p.substring(0, 1).toUpperCase() + p.substring(1));
                } catch (NoSuchMethodException e) {
                    // try next getter
                }
            }
            if (getter == null) {
                SneakyThrower.sneakyThrow(new SQLException(format("Cannot find getter for field %s in class %s", p, value.getClass())));
            }
            value = invoke(value, getter);
        }

        return value;
    }

    private Object invoke(Object obj, Method method, Object ... args) {
        try {
            return method.invoke(obj, args);
        } catch (ReflectiveOperationException e) {
            return SneakyThrower.sneakyThrow(new SQLException(e));
        }
    }
}
