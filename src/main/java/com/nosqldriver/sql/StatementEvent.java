package com.nosqldriver.sql;

public class StatementEvent {
    public StatementEvent(StatementType type, String sql) {
        this.type = type;
        this.sql = sql;
    }

    public enum StatementType {
        SELECT, INSERT, UPDATE, DELETE, TRUNCATE, SHOW, USE, CREATE_INDEX, DROP_INDEX
    }

    private final StatementType type;
    private final String sql;


    public StatementType getType() {
        return type;
    }

    public String getSql() {
        return sql;
    }
}
