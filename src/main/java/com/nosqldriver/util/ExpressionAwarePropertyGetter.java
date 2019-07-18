package com.nosqldriver.util;

import com.nosqldriver.sql.ExpressionEvaluator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public class ExpressionAwarePropertyGetter<T> implements BiFunction<T, String, Object> {
    private static final Pattern namepatern = Pattern.compile("^\\w+[\\w\\d_\\$]*$");
    private final BiFunction<T, String, Object> valueGetter;
    private final Function<T, Iterable<String>> namesLister;

    public ExpressionAwarePropertyGetter(BiFunction<T, String, Object> valueGetter, Function<T, Iterable<String>> namesLister) {
        this.valueGetter = valueGetter;
        this.namesLister = namesLister;
    }


    @Override
    public Object apply(T object, String name) {
        Object value = valueGetter.apply(object, name);
        if (value == null && isExpression(name) && find(namesLister.apply(object), name) == null) {
            Function<T, Object> evaluator = new ExpressionEvaluator<T>(name) {
                @SuppressWarnings("unchecked")
                @Override
                protected Map<String, Object> toMap(T record) {
                    if (record instanceof Map) {
                        return (Map<String, Object>)record;
                    }
                    Map<String, Object> map = new LinkedHashMap<>();
                    for (String name : namesLister.apply(record)) {
                        Object value = valueGetter.apply(record, name);
                        map.put(name, value);
                    }
                    return map;
                }
            };
            return evaluator.apply(object);
        }
        return value;
    }


    private String find(Iterable<String> names, String name) {
        for (String n : names) {
            if (Objects.equals(n, name)) {
                return name;
            }
        }
        return null;
    }

    private boolean isExpression(String str) {
        return !namepatern.matcher(str).find();
    }
}
