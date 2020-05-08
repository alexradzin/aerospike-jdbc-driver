package com.nosqldriver.util;

import com.nosqldriver.VisibleForPackage;
import org.json.simple.parser.JSONParser;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
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

@VisibleForPackage
class StandardFunctions {
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
    static final Map<String, Object> functions = new HashMap<>();
    static {
        final Function<String, Integer> stringLength = String::length;
        functions.put("len", stringLength);
        functions.put("length", stringLength);

        functions.put("ascii", (Function<String, Integer>) s -> s.length() > 0 ? (int)s.charAt(0) : null);
        functions.put("char", (Function<Integer, String>) code -> code == null ? null : new String(new char[] {(char)code.intValue()}));
        functions.put("locate", (VarargsFunction<Object, Integer>) args -> {
            String subStr = (String)args[0];
            String str = (String)args[1];
            int offset = args.length > 2 ? (Integer)args[2] - 1 : 0;
            return str.indexOf(subStr) + 1 - offset;
        });
        functions.put("instr", (BiFunction<String, String, Integer>) (str, subStr) -> str.indexOf(subStr) + 1);
        functions.put("trim", (Function<String, String>) s -> s == null ? null : s.trim());
        functions.put("ltrim", (Function<String, String>) s -> s == null ? null : s.replaceFirst("^ *", ""));
        functions.put("rtrim", (Function<String, String>) s -> s == null ? null : s.replaceFirst(" *$", ""));
        functions.put("strcmp", (BiFunction<String, String, Integer>) String::compareTo);
        functions.put("left", (BiFunction<String, Integer, String>) (s, n) -> s.substring(0, n));

        Function<String, String> toLowerCase = s -> s == null ? null : s.toLowerCase();
        functions.put("lower", toLowerCase);
        functions.put("lcase", toLowerCase);

        Function<String, String> toUpperCase = s -> s == null ? null : s.toUpperCase();
        functions.put("upper", toUpperCase);
        functions.put("ucase", toUpperCase);
        functions.put("str", (Function<Object, String>) String::valueOf);
        functions.put("space", (Function<Integer, String>) i -> i == 0 ? "" : format("%" + i + "c", ' '));
        functions.put("reverse", (Function<String, String>) s -> s == null ? null : new StringBuilder(s).reverse().toString());
        functions.put("to_base64", (Function<Object, String>) b -> b == null ? null : getEncoder().encodeToString(b instanceof String ? ((String)b).getBytes() : (byte[])b));
        functions.put("from_base64", (Function<String, byte[]>) s -> s == null ? null : java.util.Base64.getDecoder().decode(s));
        functions.put("substring", (TriFunction<String, Integer, Integer, String>) (s, start, length) -> s.substring(start - 1, length));
        functions.put("concat", (VarargsFunction<Object, String>) args -> Arrays.stream(args).map(String::valueOf).collect(Collectors.joining()));
        functions.put("concat_ws", (VarargsFunction<Object, String>) args -> Arrays.stream(args).skip(1).map(String::valueOf).collect(Collectors.joining((String)args[0])));
        functions.put("date", (VarargsFunction<Object, Date>) args -> date.apply(args.length > 0 ? args[0] : null));
        functions.put("calendar", (VarargsFunction<Object, Calendar>) StandardFunctions::calendar);
        functions.put("now", (Supplier<Long>) System::currentTimeMillis);
        functions.put("year", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.YEAR));
        functions.put("month", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.MONTH) + 1);
        functions.put("dayofmonth", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.DAY_OF_MONTH));
        functions.put("hour", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.HOUR_OF_DAY));
        functions.put("minute", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.MINUTE));
        functions.put("second", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.SECOND));
        functions.put("millisecond", (VarargsFunction<Object, Integer>) args -> calendar(args).get(Calendar.MILLISECOND));
        functions.put("epoch", (VarargsFunction<Object, Long>) args -> parse((String)args[0], args.length > 1 ? (String)args[1] : null).getTime());
        functions.put("millis", (Function<Date, Long>) Date::getTime);
        functions.put("map", (Function<String, Map<?, ?>>) json -> (Map<?, ?>)parseJson(json));
        functions.put("list", (Function<String, List<?>>) json -> dataUtil.toList(parseJson(json)));
        functions.put("array", (Function<String, Object[]>) json -> dataUtil.toArray(parseJson(json)));
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
