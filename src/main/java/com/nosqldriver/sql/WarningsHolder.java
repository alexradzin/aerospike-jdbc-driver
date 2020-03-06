package com.nosqldriver.sql;

import java.sql.SQLException;
import java.sql.SQLWarning;

public class WarningsHolder {
    private final Object lock = new Object();
    private volatile SQLWarning sqlWarning;

    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }

    public void addWarning(String msg) {
        synchronized (lock) {
            SQLWarning warning = new SQLWarning(msg);
            if (sqlWarning == null) {
                sqlWarning = warning;
            } else {
                sqlWarning.setNextWarning(warning);
            }
        }
    }
}
