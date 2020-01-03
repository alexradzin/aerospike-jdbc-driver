package com.nosqldriver.util;

public interface ThrowingProcedure<E extends Throwable> {
    void call() throws E;
}
