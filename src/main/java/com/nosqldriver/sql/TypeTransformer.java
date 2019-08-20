package com.nosqldriver.sql;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.math.BigDecimal;
import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TypeTransformer {
    private static final Map<Class<?>, Function<Number, Number>> typeTransformers = new HashMap<>();
    static {
        typeTransformers.put(Byte.class, Number::byteValue);
        typeTransformers.put(Short.class, Number::shortValue);
        typeTransformers.put(Integer.class, Number::intValue);
        typeTransformers.put(Long.class, Number::longValue);
        typeTransformers.put(Float.class, Number::floatValue);
        typeTransformers.put(Double.class, Number::doubleValue);
        typeTransformers.put(BigDecimal.class, n -> n instanceof Double ? BigDecimal.valueOf((double)n) : n instanceof Long ? BigDecimal.valueOf((long)n) : n);

        typeTransformers.put(byte.class, n -> n == null ? (byte)0 : n.byteValue());
        typeTransformers.put(short.class, n -> n == null ? (short)0 : n.shortValue());
        typeTransformers.put(int.class, n -> n == null ? 0 : n.intValue());
        typeTransformers.put(long.class, n -> n == null ? 0L : n.longValue());
        typeTransformers.put(float.class, n -> n == null ? 0.0f : n.floatValue());
        typeTransformers.put(double.class, n -> n == null ? 0.0 : n.doubleValue());
    }
    private static final DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // used by javascript engine
    private static final DateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); // used by SQL

    @SuppressWarnings("unchecked")
    public static <T> T cast(Object obj, Class<T> type) {
        if (typeTransformers.containsKey(type) && obj instanceof Number) {
            return (T) typeTransformers.get(type).apply((Number)obj);
        }

        if (obj != null && String.class.equals(type) && !(obj instanceof String)) {
            if (obj instanceof ScriptObjectMirror && "Date".equals(((ScriptObjectMirror) obj).getClassName())) {
                //[Date yyyy-MM-dd'T'HH:mm:ss.SSS'Z']
                String str = obj.toString();
                str = str.substring(6, str.length() - 1);
                return (T)sqlDateFormat.format(Date.from(Instant.parse(str)));
            }
            return (T)obj.toString();
        }
        return (T)obj;
    }
}
