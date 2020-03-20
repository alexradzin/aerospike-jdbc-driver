package com.nosqldriver.util;

import java.lang.reflect.ParameterizedType;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private void addDeserializer(String selector, Class t, Function<?, ?> deserializer) {
        deserializers.computeIfAbsent(selector, s -> new HashMap<>()).put(t, deserializer);
    }

    private void addDeserializer(String target, Class<Function<?, ?>> deserializerClass) {
        try {
            Optional<Class> rawType = getDeserializerType(deserializerClass, 0);
            if (rawType.isPresent()) {
                addDeserializer(target, rawType.get(), deserializerClass.getConstructor().newInstance());
            } else {
                throw new IllegalArgumentException(format("Class %s is not valid deserializer", deserializerClass));
            }
        } catch (ReflectiveOperationException e) {
            SneakyThrower.sneakyThrow(e);
        }
    }

    public Optional<Class> getDeserializerType(Class<? extends Function<?, ?>> deserializerClass, int index) {
        return Arrays.stream(deserializerClass.getGenericInterfaces())
                .filter(t -> t instanceof ParameterizedType)
                .map(t -> (ParameterizedType)t)
                .filter(t -> Function.class.equals(t.getRawType())).findFirst()
                .map(f -> (Class)f.getActualTypeArguments()[index]);
    }


    public void addDeserializer(String target, String deserializerClassName) {
        try {
            @SuppressWarnings("unchecked")
            Class<Function<?, ?>> deserializerClass = (Class<Function<?, ?>>) Class.forName(deserializerClassName);
            addDeserializer(target, deserializerClass);
        } catch (ClassNotFoundException e) {
            SneakyThrower.sneakyThrow(e);
        }
    }


    public <T> Optional<? extends Function<T, ?>> getDeserializer(String selector, Class<T> from) {
        //noinspection unchecked
        return createSelectors(selector).stream()
                .map(deserializers::get)
                .filter(Objects::nonNull)
                .map(m -> (Function<T, ?>)m.get(from))
                .filter(Objects::nonNull)
                .findFirst();
    }

    private Collection<String> createSelectors(String selector) {
        String[] parts = selector.split(":");
        String namespace = parts[0];
        String set = parts[1];
        String bin = parts[2];
        return Stream.of(
                selector,
                join(namespace, set, "*"),
                join(namespace, "*", bin),
                join("*", set, bin),
                join("*", set, "*"),
                join(namespace, "*", "*"),
                join("*", "*", bin),
                join("*", "*", "*"))
                .distinct().collect(Collectors.toList());
    }

    private String join(String namespace, String set, String bin) {
        return format("%s:%s:%s", namespace, set, bin);
    }

    public Collection<String> getCustomFunctionNames() {
        return customFunctions.keySet();
    }

    public Class<Function<?, ?>> getCustomFunction(String name) {
        return customFunctions.get(name);
    }
}
