package com.nosqldriver.sql;

import com.nosqldriver.util.SneakyThrower;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

public class TypeTransformer {
    private static final Map<Class, Integer> numericTypesOrder;
    private static final Map<Integer, Integer> sqlNumericTypesOrder;
    static {
        List<Class> numericTypesOrdering = Arrays.asList(Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class);
        numericTypesOrder = IntStream.range(0, numericTypesOrdering.size()).boxed().collect(Collectors.toMap(numericTypesOrdering::get, i -> i));
        List<Integer> sqlNumericTypesOrdering = Arrays.asList(Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT, Types.FLOAT, Types.DOUBLE);
        sqlNumericTypesOrder = IntStream.range(0, sqlNumericTypesOrdering.size()).boxed().collect(Collectors.toMap(sqlNumericTypesOrdering::get, i -> i));
    }
    private static final Comparator<Class> typesComparator = new Comparator<Class>() {
        @Override
        public int compare(Class o1, Class o2) {
            return numericTypesOrder.getOrDefault(o1, -1) - numericTypesOrder.getOrDefault(o2, -1);
        }
    };
    private static final Map<Class<?>, Function<Object, Object>> typeTransformers = new HashMap<>();
    static {
        typeTransformers.put(Byte.class, n -> ((Number)n).byteValue());
        typeTransformers.put(Short.class, n -> ((Number)n).shortValue());
        typeTransformers.put(Integer.class, n -> ((Number)n).intValue());
        typeTransformers.put(Long.class, n -> ((Number)n).longValue());
        typeTransformers.put(Float.class, n -> ((Number)n).floatValue());
        typeTransformers.put(Double.class, n -> ((Number)n).doubleValue());
        typeTransformers.put(BigDecimal.class, n -> n instanceof Double ? BigDecimal.valueOf((double)n) : n instanceof Long ? BigDecimal.valueOf((long)n) : n);

        typeTransformers.put(byte.class, n -> n == null ? (byte)0 : ((Number)n).byteValue());
        typeTransformers.put(short.class, n -> n == null ? (short)0 : ((Number)n).shortValue());
        typeTransformers.put(int.class, n -> n == null ? 0 : ((Number)n).intValue());
        typeTransformers.put(long.class, n -> n == null ? 0L : ((Number)n).longValue());
        typeTransformers.put(float.class, n -> n == null ? 0.0f : ((Number)n).floatValue());
        typeTransformers.put(double.class, n -> n == null ? 0.0 : ((Number)n).doubleValue());

        typeTransformers.put(Date.class, o -> o instanceof Date ? o : new Date(((Number)o).longValue()));
        typeTransformers.put(Time.class, o -> o instanceof Time ? o : new Time(((Number)o).longValue()));
        typeTransformers.put(Timestamp.class, o -> o instanceof Timestamp ? o : new Timestamp(((Number)o).longValue()));

        typeTransformers.put(boolean.class, o -> {
            if (o == null) {
                return false;
            }
            if (o instanceof Boolean) {
                return o;
            }
            if (o instanceof Number) {
                return ((Number) o).doubleValue() != 0.0;
            }
            return SneakyThrower.sneakyThrow(new SQLException(format("Cannot cast value %s to boolean", o)));
        });

        typeTransformers.put(URL.class, o -> {
            try {
                return o instanceof URL ? o : new URL((String)o);
            } catch (MalformedURLException e) {
                return SneakyThrower.sneakyThrow(new SQLException(e.getMessage(), e));
            }
        });
        typeTransformers.put(byte[].class, o -> {
            if (o instanceof String) {
                return ((String)o).getBytes();
            }
            if (!(o instanceof byte[])) {
                SneakyThrower.sneakyThrow(new SQLException(format("%s cannot be transformed to byte[]", o)));
            }
            return o;
        });
        typeTransformers.put(InputStream.class, o -> SneakyThrower.get(() -> {
            if (o instanceof byte[]) {
                return new ByteArrayInputStream((byte[])o);
            }
            if (o instanceof String) {
                return new ByteArrayInputStream(((String)o).getBytes());
            }
            if (o instanceof Blob) {
                return ((Blob)o).getBinaryStream();
            }
            if (o instanceof Clob) {
                return ((Clob)o).getAsciiStream();
            }

            return SneakyThrower.sneakyThrow(new SQLException(format("%s cannot be transformed to InputStream", o)));
        }));
        typeTransformers.put(Reader.class, o -> SneakyThrower.get(() -> o instanceof String ? new StringReader((String)o) : ((Clob)o).getCharacterStream()));
        typeTransformers.put(Blob.class, o -> {
            if (o instanceof byte[]) {
                return new ByteArrayBlob((byte[])o);
            }
            if (o instanceof Blob) {
                return o;
            }
            return SneakyThrower.sneakyThrow(new SQLException(format("%s cannot be transformed to Blob", o)));
        });
        typeTransformers.put(Clob.class, o -> {
            if (o instanceof String) {
                return new StringClob((String)o);
            }
            if (o instanceof Clob) {
                return o;
            }

            return SneakyThrower.sneakyThrow(new SQLException(format("%s cannot be transformed to Clob", o)));
        });
        typeTransformers.put(NClob.class, typeTransformers.get(Clob.class));
    }
    private static final DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // used by javascript engine
    private static final DateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); // used by SQL


    public static <T> T cast(Object obj, Class<T> type) throws SQLException {
        try {
            return safeCast(obj, type);
        } catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    public static <T> T safeCast(Object obj, Class<T> type) {
        return castImpl(obj, type, () -> {
            throw new ClassCastException(format("Class %s cannot be cast to class %s", obj.getClass(), type));
        });
    }


    public static <T> T cast(Object obj, Class<T> type, T defaultValue) {
        return castImpl(obj, type, () -> defaultValue);
    }

    @SuppressWarnings("unchecked")
    private static <T> T castImpl(Object obj, Class<T> type, Supplier<T> defaultSupplier) {
        if (typeTransformers.containsKey(type)) {
            return (T) typeTransformers.get(type).apply(obj);
        }
        if (obj != null && String.class.equals(type) && !(obj instanceof String)) {
            if (obj instanceof java.util.Date) {
                return (T)sqlDateFormat.format((java.util.Date)obj);
            }
        }
        if (obj == null || type.isAssignableFrom(obj.getClass())) {
            return (T)obj;
        }
        return defaultSupplier.get();
    }


    public static Date getDate(long epoch, Calendar cal) {
        return getDateTime(epoch, cal, Date::new);
    }

    public static Time getTime(long epoch, Calendar cal) {
        return getDateTime(epoch, cal, Time::new);
    }

    public static Timestamp getTimestamp(long epoch, Calendar cal) {
        return getDateTime(epoch, cal, Timestamp::new);
    }


    public static <R> R getDateTime(long epoch, Calendar cal, Function<Long, R> factory) {
        cal.setTime(new java.util.Date(epoch));
        return factory.apply(cal.getTime().getTime());
    }

    public static Class getMinimalType(Object value, Class barier) {
        Class type = value.getClass();
        if (!(value instanceof Number)) {
            return type;
        }
        Number n = (Number)value;
        double d = n.doubleValue();
        long l = n.longValue();
        if (d != l || Double.class.equals(barier)) {
            return Double.class;
        }
        int i = n.intValue();
        if (l != i  || Long.class.equals(barier)) {
            return Long.class;
        }
        return Integer.class;
    }

    public static Class commonType(Class c1, Class c2) {
        int comp = typesComparator.compare(c1, c2);
        return comp > 0 ? c1 : c2;
    }

    public static boolean isAssignableFrom(Class to, Class from) {
        return isAssignableFromComparison(numericTypesOrder.get(to), numericTypesOrder.get(from));
    }

    public static boolean isAssignableFrom(int to, int from) {
        return isAssignableFromComparison(sqlNumericTypesOrder.get(to), sqlNumericTypesOrder.get(from));
    }

    private static boolean isAssignableFromComparison(Integer toOrder, Integer fromOrder) {
        return toOrder != null && fromOrder != null && toOrder > fromOrder;
    }
}
