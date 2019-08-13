package com.nosqldriver.sql;

import java.sql.SQLException;

import static java.lang.String.format;

public class ExceptionFactory {
    public static SQLException invalidColumnIndex(int column) {
        return new SQLException(format("Column %d does not exist", column));
    }
}
