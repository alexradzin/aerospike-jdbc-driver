package com.nosqldriver.sql;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;

public class ResultSetWrapperFactory {
    public ResultSet create(InvocationHandler handler) {
        return (ResultSet) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{ResultSet.class}, handler);
    }
}
