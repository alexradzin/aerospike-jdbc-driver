package com.nosqldriver.util;

import java.util.function.Supplier;

@FunctionalInterface
public interface ThrowingSupplier<R, E extends Throwable> {
    R get() throws E;
}
