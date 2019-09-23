package com.nosqldriver.util;

@FunctionalInterface
public interface ThrowingSupplier<R, E extends Throwable> {
    R get() throws E;
}
