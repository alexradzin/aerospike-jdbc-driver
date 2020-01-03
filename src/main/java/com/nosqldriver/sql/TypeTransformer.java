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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;

public class TypeTransformer {
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

        typeTransformers.put(Date.class, o -> o instanceof Date ? o : new Date((Long)o));
        typeTransformers.put(Time.class, o -> o instanceof Time ? o : new Time((Long)o));
        typeTransformers.put(Timestamp.class, o -> o instanceof Timestamp ? o : new Timestamp((Long)o));

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
            throw new IllegalArgumentException(format("Cannot cast value %s to boolean", o));
        });

        typeTransformers.put(URL.class, o -> {
            try {
                return o instanceof URL ? o : new URL((String)o);
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException(e);
            }
        });
        typeTransformers.put(byte[].class, o -> {
            if (o instanceof byte[]) {
                return o;
            }
            throw new IllegalArgumentException(format("%s cannot be transformed to byte[]", o));
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

            throw new IllegalArgumentException(format("%s cannot be transformed to InputStream", o));
        }));
        typeTransformers.put(Reader.class, o -> SneakyThrower.get(() -> o instanceof String ? new StringReader((String)o) : ((Clob)o).getCharacterStream()));
        typeTransformers.put(Blob.class, o -> {
            if (o instanceof byte[]) {
                return new ByteArrayBlob((byte[])o);
            }
            if (o instanceof Blob) {
                return o;
            }
            throw new IllegalArgumentException(format("%s cannot be transformed to InputStream", o));
        });
        typeTransformers.put(Clob.class, o -> {
            if (o instanceof String) {
                return new StringClob((String)o);
            }
            if (o instanceof Clob) {
                return o;
            }

            throw new IllegalArgumentException(format("%s cannot be transformed to InputStream", o));
        });
        typeTransformers.put(NClob.class, typeTransformers.get(Clob.class));
    }
    private static final DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"); // used by javascript engine
    private static final DateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); // used by SQL


    public static <T> T cast(Object obj, Class<T> type) throws SQLException {
        try {
            return castImpl(obj, type);
        } catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T castImpl(Object obj, Class<T> type) {
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
        throw new ClassCastException(format("lass %s cannot be cast to class %s", obj.getClass(), type));
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
}
