package com.nosqldriver.sql;


import com.aerospike.client.policy.Policy;

public class DriverPolicy extends Policy {
    public enum Script {
        js, lua
    }

    public Script script;
    public boolean sendKeyDigest;
    public boolean sendGeneration;
    public boolean sendExpiration;

    public Script getScript() {
        return script;
    }
}
