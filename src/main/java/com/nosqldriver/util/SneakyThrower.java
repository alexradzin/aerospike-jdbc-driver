package com.nosqldriver.util;

import java.sql.SQLException;

public class SneakyThrower {
    @SuppressWarnings("unchecked")
    public static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    public static <R> R get(ThrowingSupplier<R, SQLException> supplier) {
        try {
            return supplier.get();
        } catch (SQLException e) {
            sneakyThrow(e);
            return null;
        }
    }
}
