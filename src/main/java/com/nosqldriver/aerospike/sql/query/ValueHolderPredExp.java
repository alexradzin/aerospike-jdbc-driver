package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;
import com.nosqldriver.sql.SqlLiterals;
import com.nosqldriver.util.SneakyThrower;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.stream.IntStream;

public class ValueHolderPredExp<T> extends FakePredExp {
    private final T value;

    public static PredExp create(Object val) {
        if(val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
            return PredExp.integerValue(((Number)val).longValue());
        }
        if (val instanceof Calendar) {
            return PredExp.integerValue((Calendar)val);
        }
        if(val instanceof String) {
            return PredExp.stringValue((String)val);
        }
        if(val instanceof short[]) {
            return createShortArrayHolder((short[])val);
        }
        if(val instanceof int[]) {
            return createIntArrayHolder((int[])val);
        }
        if(val instanceof long[]) {
            return createLongArrayHolder((long[])val);
        }
        if(val.getClass().isArray()) {
            return createArrayHolder((Object[])val);
        }
        if(val instanceof Array) {
            try {
                return createSqlArrayHolder((Array)val);
            } catch (SQLException e) {
                SneakyThrower.sneakyThrow(e);
            }
        }
        throw new IllegalArgumentException("" + val);
    }


    public static ValueHolderPredExp<short[]> createShortArrayHolder(short[] shorts) {
        return new ValueHolderPredExp<>(shorts);
    }

    public static ValueHolderPredExp<int[]> createIntArrayHolder(int[] ints) {
        return new ValueHolderPredExp<>(ints);
    }

    public static ValueHolderPredExp<long[]> createLongArrayHolder(long[] longs) {
        return new ValueHolderPredExp<>(longs);
    }

    public static <T> ValueHolderPredExp<T[]> createArrayHolder(T[] numbers) {
        return new ValueHolderPredExp<>(numbers);
    }

    public static <T> ValueHolderPredExp<T[]> createSqlArrayHolder(Array array) throws SQLException {
        int sqlType = array.getBaseType();
        Class elementType = SqlLiterals.sqlToJavaTypes.get(sqlType);
        Object values = array.getArray();
        int n = java.lang.reflect.Array.getLength(values);
        @SuppressWarnings("unchecked")
        T[] valuesArray = (T[])java.lang.reflect.Array.newInstance(elementType, n);
        IntStream.range(0, n).forEach(i -> java.lang.reflect.Array.set(valuesArray, i, java.lang.reflect.Array.get(values, i)));
        return new ValueHolderPredExp<>(valuesArray);
    }

    public ValueHolderPredExp(T value) {
        this.value = value;
    }

    public T getValue() {
        return value;
    }

}
