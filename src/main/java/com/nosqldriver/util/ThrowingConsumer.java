package com.nosqldriver.util;

public interface ThrowingConsumer<T, E extends Throwable> {
    void accept(T t) throws E;
}
