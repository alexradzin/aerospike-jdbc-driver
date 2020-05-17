package com.nosqldriver.sql;

public class DriverPolicy {
    public enum Script {
        js, lua
    }

    public Script script;

    public Script getScript() {
        return script;
    }
}
