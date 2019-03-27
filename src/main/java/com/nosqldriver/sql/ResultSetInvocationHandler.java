package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.AerospikeResultSetMetaData;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class ResultSetInvocationHandler<R> implements InvocationHandler {
    public static final int NEXT = 1;
    public static final int METADATA = 2;
    public static final int GET_INDEX = 4;
    public static final int GET_NAME = 8;
    public static final int OTHER = 8;

    protected final R resultSet;
    private final String schema;
    private final String[] names;
    private final String[] aliases;
    private final int features;


    public ResultSetInvocationHandler(int features, R resultSet, String schema, String[] names, String[] aliases) {
        this.features = features;
        this.resultSet = resultSet;
        this.schema = schema;
        this.names = names;
        this.aliases = aliases;
    }


    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if ((features & NEXT) != 0 && isNext(method)) {
            return next();
        }
        if ((features & METADATA) != 0 && isMetadata(method)) {
            return getMetadata();
        }
        if ((features & GET_INDEX) != 0 && isGetByIndex(method)) {
            return get((int)args[0], method.getReturnType());
        }
        if ((features & GET_NAME) != 0 && isGetByName(method)) {
            return get((String)args[0], method.getReturnType());
        }
        if ((features & OTHER) != 0) {
            return other(method, args);
        }
        throw new IllegalStateException("There is no handler for " + method);
    }

    protected boolean next() throws SQLException {
        throw new IllegalStateException("This method should not be called");
    }

    protected ResultSetMetaData getMetadata() throws SQLException {
        return new AerospikeResultSetMetaData(null, schema, names, aliases);
    }

    protected <T> T get(int index, Class<T> type) {
        throw new IllegalStateException("This method should not be called");
    }

    protected <T> T get(String name, Class<T> type) {
        throw new IllegalStateException("This method should not be called");
    }


    @SuppressWarnings("unchecked")
    protected <T> T other(Method method, Object[] args) throws ReflectiveOperationException {
        return (T)method.invoke(resultSet, args);
    }

    protected boolean isNext(Method method) {
        return "next".equals(method.getName()) && boolean.class.equals(method.getReturnType()) && method.getParameterTypes().length == 0;
    }
    protected boolean isMetadata(Method method) {
        return "getMetaData".equals(method.getName()) && ResultSetMetaData.class.equals(method.getReturnType()) && method.getParameterTypes().length == 0;
    }
    protected boolean isGetByIndex(Method method) {
        return isGet(method, int.class);
    }
    protected boolean isGetByName(Method method) {
        return isGet(method, String.class);
    }

    private boolean isGet(Method method, Class<?> paramType) {
        return method.getName().startsWith("get") && method.getParameterTypes().length == 1 && paramType.equals(method.getParameterTypes()[0]);
    }
}
