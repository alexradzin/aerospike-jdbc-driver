package com.nosqldriver.util;

import javax.script.ScriptException;
import java.sql.SQLException;

public class SneakyThrower {
    @SuppressWarnings("unchecked")
    public static <R, E extends Throwable> R sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    public static <R> R get(ThrowingSupplier<R, SQLException> supplier) {
        try {
            return supplier.get();
        } catch (SQLException e) {
            return sneakyThrow(e);
        }
    }

    public static void sqlCall(ThrowingProcedure<SQLException> p) {
        try {
            p.call();
        } catch (SQLException e) {
            sneakyThrow(e);
        }
    }

    public static void call(ThrowingProcedure<ScriptException> p) {
        try {
            p.call();
        } catch (ScriptException | RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                sneakyThrow(e.getCause());
            }
            sneakyThrow(new SQLException(e.getMessage(), e));
        }
    }
}
