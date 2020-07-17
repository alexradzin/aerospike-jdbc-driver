package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;
import com.nosqldriver.sql.SqlLiterals;
import com.nosqldriver.util.SneakyThrower;

import java.sql.Array;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

public class ValueHolderPredExp<T> extends FakePredExp {
    private static final Map<Class, Function<Object, PredExp>> predExpFactories = new HashMap<>();
    static {
        Function<Object, PredExp> intFactory = val -> PredExp.integerValue(((Number)val).longValue());
        for (Class c : new Class[] {Byte.class, Short.class, Integer.class, Long.class, byte.class, short.class, int.class, long.class}) {
            predExpFactories.put(c, intFactory);
        }
        predExpFactories.put(String.class, val -> PredExp.stringValue((String)val));
        predExpFactories.put(short[].class, val -> new ValueHolderPredExp<>((short[])val));
        predExpFactories.put(int[].class, val -> new ValueHolderPredExp<>((int[])val));
        predExpFactories.put(long[].class, val -> new ValueHolderPredExp<>((long[])val));
    }
    private final T value;


    public static PredExp create(Object val) {
        if (val == null) {
            SneakyThrower.sneakyThrow(new SQLException("Predicate value cannot be null"));
        }
        Function<Object, PredExp> factory = predExpFactories.get(val.getClass());
        if (factory != null) {
            return factory.apply(val);
        }
        if (val instanceof byte[]) {
            return new ValueHolderPredExp<>((byte[])val);
        }
        if (val.getClass().isArray()) {
            return new ValueHolderPredExp<>((Object[])val);
        }
        if (val instanceof Array) {
            return SneakyThrower.get(() -> createSqlArrayHolder((Array)val));
        }
        return SneakyThrower.sneakyThrow(new SQLException("" + val));
    }


    private static <T> ValueHolderPredExp<T[]> createSqlArrayHolder(Array array) throws SQLException {
        int sqlType = array.getBaseType();
        Class elementType = SqlLiterals.sqlToJavaTypes.get(sqlType);
        Object values = array.getArray();
        int n = java.lang.reflect.Array.getLength(values);
        @SuppressWarnings("unchecked")
        T[] valuesArray = (T[])java.lang.reflect.Array.newInstance(elementType, n);
        IntStream.range(0, n).forEach(i -> java.lang.reflect.Array.set(valuesArray, i, java.lang.reflect.Array.get(values, i)));
        return new ValueHolderPredExp<>(valuesArray);
    }

    private ValueHolderPredExp(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }
}
