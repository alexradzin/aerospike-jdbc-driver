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

}
