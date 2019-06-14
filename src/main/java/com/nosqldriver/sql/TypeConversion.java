package com.nosqldriver.sql;

import java.sql.Date;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class TypeConversion {
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
}
