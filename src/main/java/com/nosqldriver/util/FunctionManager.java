package com.nosqldriver.util;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.lang.String.format;

public class FunctionManager {
    private final Map<String, Object> functions;

    public FunctionManager() {
        this(StandardFunctions.functions);
    }

    public FunctionManager(Map<String, Object> functions) {
        this.functions = functions;
    }

    public void addFunction(String name, String className) {
        try {
            @SuppressWarnings("unchecked")
            Class<Function<?, ?>> clazz = (Class<Function<?, ?>>) Class.forName(className);
            if (!Function.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(format("Class %s is not valid function because it does not implement interface %s", className, Function.class));
            }
            addFunction(name, clazz);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(format("Class %s is not valid function", className));
        }
    }

    public void addFunction(String name, Class<Function<?, ?>> clazz) {
        try {
            addFunction(name, clazz.getConstructor().newInstance());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }


    }

    public void addFunction(String name, Object function) {
        functions.put(name, function);
    }

    public Optional<Class> getFunctionReturnType(String name) {
        return Optional.ofNullable(functions.get(name)).flatMap(f -> getFunctionReturnType(f.getClass()));
    }

    private Optional<Class> getFunctionReturnType(Class deserializerClass) {
        return getFunctionType(deserializerClass, 1);
    }


    private Optional<Class> getFunctionType(Class deserializerClass, int index) {
        return Arrays.stream(deserializerClass.getGenericInterfaces())
                .filter(t -> t instanceof ParameterizedType)
                .map(t -> (ParameterizedType)t)
                .filter(t -> isFunction(t.getRawType())).findFirst()
                .map(f -> (Class)f.getActualTypeArguments()[index]);
    }

    private boolean isFunction(Type type) {
        return Function.class.equals(type) || BiFunction.class.equals(type) || TriFunction.class.equals(type) || VarargsFunction.class.equals(type);
    }

    public Collection<String> getFunctionNames() {
        return functions.keySet();
    }

    public <F> F getFunction(String name) {
        //noinspection unchecked
        return (F)functions.get(name);
    }
}
