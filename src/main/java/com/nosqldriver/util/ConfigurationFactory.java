package com.nosqldriver.util;

import com.aerospike.client.policy.Policy;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class ConfigurationFactory {
    private static final Map<Class, Function<String, Object>> typeTransformers = new HashMap<>();
    static {
        typeTransformers.put(byte.class, Byte::parseByte);
        typeTransformers.put(Byte.class, Byte::parseByte);
        typeTransformers.put(short.class, Short::parseShort);
        typeTransformers.put(Short.class, Short::parseShort);
        typeTransformers.put(int.class, Integer::parseInt);
        typeTransformers.put(Integer.class, Integer::parseInt);
        typeTransformers.put(long.class, Long::parseLong);
        typeTransformers.put(Long.class, Long::parseLong);
        typeTransformers.put(boolean.class, Boolean::parseBoolean);
        typeTransformers.put(Boolean.class, Boolean::parseBoolean);
        typeTransformers.put(float.class, Float::parseFloat);
        typeTransformers.put(Float.class, Float::parseFloat);
        typeTransformers.put(double.class, Double::parseDouble);
        typeTransformers.put(Double.class, Double::parseDouble);
        typeTransformers.put(String.class, s -> s);
    }



    public static Policy copy(Policy src, Policy dst) {
        Class<? extends Policy> clazz = dst.getClass();
        Arrays.stream(src.getClass().getFields())
                .forEach(f -> {
                    try {
                        set(clazz, f.getName(), f.get(src), dst);
                    } catch (ReflectiveOperationException e) {
                        // ignore it
                    }
                });
        return dst;
    }

    public static <T> T copy(Properties props, T object) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>)object.getClass();
        props.forEach((key, value) -> {
            try {
                set(clazz, (String)key, (String)value, object);
            } catch (ReflectiveOperationException e1) {
                // ignore it; this property does not belong to ClientPolicy
            }
        });

        return object;
    }

    private static void set(Class clazz, String key, String strValue, Object object) throws ReflectiveOperationException {
        Field field = clazz.getField(key);
        Function<String, Object> transformer = typeTransformers.get(field.getType());
        Object value = null;
        if (transformer != null) {
            value = transformer.apply(strValue);
        } else if (field.getType().isEnum()) {
            //noinspection unchecked
            value = Enum.valueOf((Class<? extends Enum>)field.getType(), strValue);
        } else {
            throw new IllegalAccessException(field.getType().getName());
        }
        field.set(object, value);
    }

    private static void set(Class clazz, String key, Object value, Object object) throws ReflectiveOperationException {
        clazz.getField(key).set(object, value);
    }

}
