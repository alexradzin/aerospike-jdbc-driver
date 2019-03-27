package com.nosqldriver.sql;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TypeTransformer {
    private static final Map<Class<?>, Function<Number, Number>> typeTransformers = new HashMap<>();
    static {
        typeTransformers.put(Byte.class, Number::byteValue);
        typeTransformers.put(Short.class, Number::shortValue);
        typeTransformers.put(Integer.class, Number::intValue);
        typeTransformers.put(Long.class, Number::longValue);
        typeTransformers.put(Float.class, Number::floatValue);
        typeTransformers.put(Double.class, Number::doubleValue);

        typeTransformers.put(byte.class, Number::byteValue);
        typeTransformers.put(short.class, Number::shortValue);
        typeTransformers.put(int.class, Number::intValue);
        typeTransformers.put(long.class, Number::longValue);
        typeTransformers.put(float.class, Number::floatValue);
        typeTransformers.put(double.class, Number::doubleValue);
    }

    @SuppressWarnings("unchecked")
    public static <T> T cast(Object obj, Class<T> type) {
        if (typeTransformers.containsKey(type) && obj instanceof Number) {
            return (T) typeTransformers.get(type).apply((Number)obj);
        }

        if (typeTransformers.containsKey(type) && obj instanceof String) {
            return (T)Integer.valueOf((String)obj); //TODO: patch!!!
        }
        return (T)obj;
    }

}
