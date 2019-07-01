package com.nosqldriver.sql;

import com.aerospike.client.query.PredExp;

import java.sql.Date;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Supplier;

public class SqlLiterals {
    public static final Map<Class, Integer> sqlTypes = new HashMap<>();
    static {
        sqlTypes.put(Short.class, Types.SMALLINT);
        sqlTypes.put(Integer.class, Types.INTEGER);
        sqlTypes.put(Long.class, Types.BIGINT);
        sqlTypes.put(Boolean.class, Types.BOOLEAN);
        sqlTypes.put(Float.class, Types.FLOAT);
        sqlTypes.put(Double.class, Types.DOUBLE);
        sqlTypes.put(String.class, Types.VARCHAR);
        sqlTypes.put(byte[].class, Types.BLOB);
        sqlTypes.put(Date.class, Types.DATE);
    }

    public static final Map<Integer, String> sqlTypeNames = new HashMap<>();
    static {
        sqlTypeNames.put(Types.SMALLINT, "short");
        sqlTypeNames.put(Types.INTEGER, "integer");
        sqlTypeNames.put(Types.BIGINT, "long");
        sqlTypeNames.put(Types.BOOLEAN, "boolean");
        sqlTypeNames.put(Types.FLOAT, "float");
        sqlTypeNames.put(Types.DOUBLE, "double");
        sqlTypeNames.put(Types.VARCHAR, "varchar");
        sqlTypeNames.put(Types.BLOB, "blob");
        sqlTypeNames.put(Types.DATE, "date");
    }

    public static final Map<Integer, Class> sqlToJavaTypes = new HashMap<>();
    static {
        sqlToJavaTypes.put(Types.SMALLINT, Short.class);
        sqlToJavaTypes.put(Types.INTEGER, Integer.class);
        sqlToJavaTypes.put(Types.BIGINT, Long.class);
        sqlToJavaTypes.put(Types.BOOLEAN, Boolean.class);
        sqlToJavaTypes.put(Types.FLOAT, Float.class);
        sqlToJavaTypes.put(Types.DOUBLE, Double.class);
        sqlToJavaTypes.put(Types.VARCHAR, String.class);
        sqlToJavaTypes.put(Types.LONGVARCHAR, String.class);
        sqlToJavaTypes.put(Types.BLOB, byte[].class);
        sqlToJavaTypes.put(Types.BINARY, byte[].class);
        sqlToJavaTypes.put(Types.VARBINARY, byte[].class);
        sqlToJavaTypes.put(Types.LONGVARBINARY, byte[].class);
        sqlToJavaTypes.put(Types.DATE, java.sql.Date.class);
        sqlToJavaTypes.put(Types.TIME, java.sql.Time.class);
        sqlToJavaTypes.put(Types.TIMESTAMP, java.sql.Timestamp.class);
    }

    public static final Map<String, Supplier<PredExp>> predExpOperators = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        predExpOperators.put(operatorKey(String.class, "="), PredExp::stringEqual);
        predExpOperators.put(operatorKey(String.class, "<>"), PredExp::stringUnequal);
        predExpOperators.put(operatorKey(String.class, "!="), PredExp::stringUnequal);
        predExpOperators.put(operatorKey(String.class, "LIKE"), () -> PredExp.stringRegex(0));
        predExpOperators.put(operatorKey(String.class, "AND"), () -> PredExp.and(2));
        predExpOperators.put(operatorKey(String.class, "OR"), () -> PredExp.or(2));

        for (Class type : new Class[] {Byte.class, Short.class, Integer.class, Long.class}) {
            predExpOperators.put(operatorKey(type, "="), PredExp::integerEqual);
            predExpOperators.put(operatorKey(type, "<>"), PredExp::integerUnequal);
            predExpOperators.put(operatorKey(type, "!="), PredExp::integerUnequal);
            predExpOperators.put(operatorKey(type, ">"), PredExp::integerGreater);
            predExpOperators.put(operatorKey(type, ">="), PredExp::integerGreaterEq);
            predExpOperators.put(operatorKey(type, "<"), PredExp::integerLess);
            predExpOperators.put(operatorKey(type, "<="), PredExp::integerLessEq);
            predExpOperators.put(operatorKey(type, "AND"), () -> PredExp.and(2));
            predExpOperators.put(operatorKey(type, "OR"), () -> PredExp.or(2));
        }

    }

    public static String operatorKey(Class<?> type, String operand) {
        return type.getName() + operand;
    }
}
