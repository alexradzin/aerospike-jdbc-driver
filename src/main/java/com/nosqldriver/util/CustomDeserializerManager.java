package com.nosqldriver.util;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.lang.String.format;

public class CustomDeserializerManager {
    private final Map<String, Map<Class, Function<?, ?>>> deserializers = new HashMap<>();
    private final Map<String, Class<Function<?, ?>>> customFunctions = new HashMap<>();

    public void addCustomFunction(String name, String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<Function<?, ?>> clazz = (Class<Function<?, ?>>) Class.forName(className);
            addCustomFunction(name, clazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(format("Class %s is not valid function", className));
        }
    }

    public void addCustomFunction(String name, Class<Function<?, ?>> clazz) {
        customFunctions.put(name, clazz);
    }

    public Optional<Class> getDeserializerType(Class<? extends Function<?, ?>> deserializerClass, int index) {
        return Arrays.stream(deserializerClass.getGenericInterfaces())
                .filter(t -> t instanceof ParameterizedType)
                .map(t -> (ParameterizedType)t)
                .filter(t -> Function.class.equals(t.getRawType())).findFirst()
                .map(f -> (Class)f.getActualTypeArguments()[index]);
    }

    public Collection<String> getCustomFunctionNames() {
        return customFunctions.keySet();
    }

    public Class<Function<?, ?>> getCustomFunction(String name) {
        return customFunctions.get(name);
    }
}
