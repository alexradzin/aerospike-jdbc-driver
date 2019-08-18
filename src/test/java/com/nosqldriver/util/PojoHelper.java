package com.nosqldriver.util;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class PojoHelper {
    public static Map<String, Object> fieldsToMap(Object obj) {
        Map<String, Object> map = new LinkedHashMap<>();
        for(Field field : obj.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            try {
                map.put(field.getName(), field.get(obj));
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
        return map;
    }


    public static Collection<String> fieldNames(Object obj) {
        return Arrays.stream(obj.getClass().getDeclaredFields()).map(f -> {f.setAccessible(true); return f.getName();}).collect(toList());
    }

    public static List<Object> fieldValues(final Object obj) {
        return Arrays.stream(obj.getClass().getDeclaredFields()).map(f -> {
            try {
                f.setAccessible(true);
                return f.get(obj);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }).collect(toList());
    }
}
