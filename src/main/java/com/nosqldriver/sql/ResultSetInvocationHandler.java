package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.AerospikeResultSetMetaData;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class ResultSetInvocationHandler<R> implements InvocationHandler {
    public static final int NEXT = 1;
    public static final int METADATA = 2;
    public static final int GET_INDEX = 4;
    public static final int GET_NAME = 8;
    public static final int OTHER = 8;

    static final Map<Class<?>, Function<Number, Number>> typeTransforemers = new HashMap<>();
    static {
        typeTransforemers.put(Byte.class, Number::byteValue);
        typeTransforemers.put(Short.class, Number::shortValue);
        typeTransforemers.put(Integer.class, Number::intValue);
        typeTransforemers.put(Long.class, Number::longValue);
        typeTransforemers.put(Float.class, Number::floatValue);
        typeTransforemers.put(Double.class, Number::doubleValue);

        typeTransforemers.put(byte.class, Number::byteValue);
        typeTransforemers.put(short.class, Number::shortValue);
        typeTransforemers.put(int.class, Number::intValue);
        typeTransforemers.put(long.class, Number::longValue);
        typeTransforemers.put(float.class, Number::floatValue);
        typeTransforemers.put(double.class, Number::doubleValue);
    }


    private final R resultSet;
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


    @SuppressWarnings("unchecked")
    protected <T> T cast(Object obj, Class<T> type) {
        if (typeTransforemers.containsKey(type) && obj instanceof Number) {
            return (T)typeTransforemers.get(type).apply((Number)obj);
        }

        if (typeTransforemers.containsKey(type) && obj instanceof String) {
            return (T)Integer.valueOf((String)obj); //TODO: patch!!!
        }
        return (T)obj;
    }

}
