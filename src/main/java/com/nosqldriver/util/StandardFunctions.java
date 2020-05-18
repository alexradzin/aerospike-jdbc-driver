package com.nosqldriver.util;

import com.nosqldriver.VisibleForPackage;
import org.json.simple.parser.JSONParser;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.nosqldriver.util.DateParser.*;
import static java.lang.String.format;
import static java.util.Base64.*;

// Only functions allow discovering generic types of arguments; lambdas do not allow this.
@SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
@VisibleForPackage
public class StandardFunctions {
    private static DataUtil dataUtil = new DataUtil();

    private static Function<Object, Date> date = arg -> {
        if (arg instanceof Date) {
            return (Date)arg;
        }
        if (arg == null) {
            return new Date();
        }
        if (arg instanceof Number) {
            return new Date(((Number)arg).longValue());
        }
        if (arg instanceof String) {
            return parse((String) arg, null);
        }
        return SneakyThrower.sneakyThrow(new SQLException("Wrong argument " + arg + " type: " + (arg.getClass())));
    };

    @VisibleForPackage
    public static final Map<String, Object> functions = new HashMap<>();
    static {
        final Function<Object, Integer> objLength = new Function<Object, Integer>() {
            @Override
            public Integer apply(Object o) {
                if (o == null) {
                    return 0;
                }
                if (o instanceof CharSequence) {
                    return ((CharSequence)o).length();
                }
                if (o instanceof Collection) {
                    return ((Collection)o).size();
                }
                if (o instanceof Map) {
                    return ((Map)o).size();
                }
                throw new IllegalArgumentException("function length() does not support " + o);
            }
        };
        functions.put("len", objLength);
        functions.put("length", objLength);

        functions.put("ascii", new Function<String, Integer>() {
            @Override
            public Integer apply(String s) {
                return s.length() > 0 ? (int) s.charAt(0) : null;
            }
        });
        functions.put("char", new Function<Integer, String>() {
            @Override
            public String apply(Integer code) {
                return code == null ? null : new String(new char[] {(char)code.intValue()});
            }
        });
        functions.put("locate", (VarargsFunction<Object, Integer>) args -> {
            String subStr = (String)args[0];
            String str = (String)args[1];
            int offset = args.length > 2 ? (Integer)args[2] - 1 : 0;
            return str.indexOf(subStr) + 1 - offset;
        });
        functions.put("instr", new BiFunction<String, String, Integer>() {
            @Override
            public Integer apply(String str, String subStr) {
                return str.indexOf(subStr) + 1;
            }
        });
        functions.put("trim", new Function<String, String>() {

            @Override
            public String apply(String s) {
                return s == null ? null : s.trim();
            }
        });
        functions.put("ltrim", new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s == null ? null : s.replaceFirst("^ *", "");
            }
        });
        functions.put("rtrim", new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s == null ? null : s.replaceFirst(" *$", "");
            }
        });
        functions.put("strcmp", new BiFunction<String, String, Integer>() {

            @Override
            public Integer apply(String s, String s2) {
                return s.compareTo(s2);
            }
        });
        functions.put("left", new BiFunction<String, Integer, String>() {
            @Override
            public String apply(String s, Integer n) {
                return s.substring(0, n);
            }
        });

        Function<String, String> toLowerCase = new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s == null ? null : s.toLowerCase();
            }
        };
        functions.put("lower", toLowerCase);
        functions.put("lcase", toLowerCase);

        Function<String, String> toUpperCase = new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s == null ? null : s.toUpperCase();
            }
        };
        functions.put("upper", toUpperCase);
        functions.put("ucase", toUpperCase);

        functions.put("str", new Function<Object, String>() {
            @Override
            public String apply(Object o) {
                return String.valueOf(o);
            }
        });
        functions.put("space", new Function<Integer, String>() {
            @Override
            public String apply(Integer i) {
                return i == 0 ? "" : format("%" + i + "c", ' ');
            }
        });
        functions.put("reverse", new Function<String, String>() {
            @Override
            public String apply(String s) {
                return s == null ? null : new StringBuilder(s).reverse().toString();
            }
        });
        functions.put("to_base64", new Function<Object, String>() {
            @Override
            public String apply(Object b) {
                return b == null ? null : getEncoder().encodeToString(b instanceof String ? ((String)b).getBytes() : (byte[])b);
            }
        });
        functions.put("from_base64", new Function<String, byte[]>() {
            @Override
            public byte[] apply(String s) {
                return s == null ? null : java.util.Base64.getDecoder().decode(s);
            }
        });
        functions.put("substring", new TriFunction<String, Integer, Integer, String>() {

            @Override
            public String apply(String s, Integer start, Integer length) {
                return s.substring(start - 1, length);
            }
        });
        functions.put("concat", new VarargsFunction<Object, String>() {
            @Override
            public String apply(Object... args) {
                return Arrays.stream(args).map(String::valueOf).collect(Collectors.joining());
            }
        });
        functions.put("concat_ws", new VarargsFunction<Object, String>() {
            @Override
            public String apply(Object... args) {
                return Arrays.stream(args).skip(1).map(String::valueOf).collect(Collectors.joining((String)args[0]));
            }
        });
        functions.put("date", new VarargsFunction<Object, Date>() {
            @Override
            public Date apply(Object... args) {
                return date.apply(args.length > 0 ? args[0] : null);
            }
        });
        functions.put("calendar", new VarargsFunction<Object, Calendar>() {
            @Override
            public Calendar apply(Object... args) {
                return calendar(args);
            }
        });
        functions.put("now", new Supplier<Long>() {
            @Override
            public Long get() {
                return System.currentTimeMillis();
            }
        });
        functions.put("year", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.YEAR);
            }
        });
        functions.put("month", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.MONTH) + 1;
            }
        });
        functions.put("dayofmonth", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.DAY_OF_MONTH);
            }
        });
        functions.put("hour", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.HOUR_OF_DAY);
            }
        });
        functions.put("minute", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.MINUTE);
            }
        });
        functions.put("second", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.SECOND);
            }
        });
        functions.put("millisecond", new VarargsFunction<Object, Integer>() {
            @Override
            public Integer apply(Object... args) {
                return calendar(args).get(Calendar.MILLISECOND);
            }
        });
        functions.put("epoch", new VarargsFunction<Object, Long>() {
            @Override
            public Long apply(Object... args) {
                return parse((String)args[0], args.length > 1 ? (String)args[1] : null).getTime();
            }
        });
        functions.put("millis", new Function<Date, Long>() {
            @Override
            public Long apply(Date date) {
                return date.getTime();
            }
        });
        functions.put("map", new Function<String, Map<?, ?>>() {
            @Override
            public Map<?, ?> apply(String json) {
                return (Map<?, ?>)parseJson(json);
            }
        });
        functions.put("list", new Function<String, List<?>>() {
            @Override
            public List<?> apply(String json) {
                return dataUtil.toList(parseJson(json));
            }
        });
        functions.put("array", new Function<String, Object[]>() {
            @Override
            public Object[] apply(String json) {
                return dataUtil.toArray(parseJson(json));
            }
        });
    }


    private static Date parse(String str, String fmt) {
        try {
            Date d = fmt != null ? new SimpleDateFormat(fmt).parse(str) : date(str);
            return new Date(d.getTime());
        } catch (ParseException e) {
            return SneakyThrower.sneakyThrow(new SQLException(e));
        }
    }

    private static Calendar calendar(Object[] args) {
        Date d = date.apply(args.length > 0 ? args[0] : null);
        Calendar c = Calendar.getInstance();
        c.setTime(d);
        return c;
    }

    private static <T> T parseJson(String json) {
        try {
            //noinspection unchecked
            return (T)new JSONParser().parse(json);
        } catch (org.json.simple.parser.ParseException e) {
            return SneakyThrower.sneakyThrow(new SQLException(e));
        }
    }
}
