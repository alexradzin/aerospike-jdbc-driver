package com.nosqldriver.util;

@FunctionalInterface
public interface VarargsFunction<T, R> {
    @SuppressWarnings({"VariableArgumentMethod", "unchecked"})
    R apply(T ... t);
}
