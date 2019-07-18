package com.nosqldriver.sql;

import com.nosqldriver.sql.OrderItem.Direction;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class OrderItemsComparator<T> implements Comparator<T> {
    private final List<OrderItem> orderItems;
    private final BiFunction<T, String, ?> getter;

    private static final Map<Class, Comparator> typedComparators = new HashMap<>();
    static {
        typedComparators.put(Byte.class, (Comparator<Byte>) (x, y) -> nullSafeCall(Byte::compare, x, y));
        typedComparators.put(Short.class, (Comparator<Short>) (x, y) -> nullSafeCall(Short::compare, x, y));
        typedComparators.put(Integer.class, (Comparator<Integer>) (x, y) -> nullSafeCall(Integer::compare, x, y));
        typedComparators.put(Long.class, (Comparator<Long>) (x, y) -> nullSafeCall(Long::compare, x, y));
        typedComparators.put(Float.class, (Comparator<Float>) (x, y) -> nullSafeCall(Float::compare, x, y));
        typedComparators.put(Double.class, (Comparator<Double>) (x, y) -> nullSafeCall(Double::compare, x, y));
        typedComparators.put(Boolean.class, (Comparator<Boolean>) (x, y) -> nullSafeCall(Boolean::compare, x, y));
        typedComparators.put(String.class, (Comparator<String>) (x, y) -> nullSafeCall(String::compareTo, x, y));
        typedComparators.put(byte[].class, (Comparator<byte[]>) (x1, y1) -> nullSafeCall((x, y) -> IntStream.range(0, Math.min(x.length, y.length)).boxed().map(i -> x[i] - y[i]).filter(c -> c != 0).findFirst().orElseGet(() -> Integer.compare(x.length, y.length)), x1, y1));
        typedComparators.put(java.util.Date.class, (Comparator<java.util.Date>) (x, y) -> nullSafeCall(java.util.Date::compareTo, x, y));
        typedComparators.put(Date.class, (Comparator<Date>) (x1, y1) -> nullSafeCall((x, y) -> x.toLocalDate().compareTo(y.toLocalDate()), x1, y1));
        typedComparators.put(Time.class, (Comparator<Time>) (x1, y1) -> nullSafeCall((x, y) -> x.toLocalTime().compareTo(y.toLocalTime()), x1, y1));
        typedComparators.put(Timestamp.class, (Comparator<Timestamp>) (x1, y1) -> nullSafeCall((x, y) -> x.toLocalDateTime().compareTo(y.toLocalDateTime()), x1, y1));
    }
    private static Map<Direction, Integer> sign = new EnumMap<>(Direction.class);
    static {
        sign.put(Direction.ASC, 1);
        sign.put(Direction.DESC, -1);
    }
    private static AtomicInteger index = new AtomicInteger(0);
    private static final Map<Class, Integer> typeOrder = Stream.of(Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class, Date.class, Time.class, Timestamp.class, java.util.Date.class, String.class, byte[].class).collect(toMap(c -> c, c -> index.getAndIncrement()));


    private static final Map<Class, Function> typedConverters = new HashMap<>();
    static {
        typedConverters.put(Byte.class, (Function<Number, Byte>) n -> nullSafeCall(Number::byteValue, n));
        typedConverters.put(Short.class, (Function<Number, Short>) n -> nullSafeCall(Number::shortValue, n));
        typedConverters.put(Integer.class, (Function<Number, Integer>) n -> nullSafeCall(Number::intValue, n));
        typedConverters.put(Long.class, (Function<Number, Long>) n -> nullSafeCall(Number::longValue, n));
        typedConverters.put(Float.class, (Function<Number, Float>) n -> nullSafeCall(Number::floatValue, n));
        typedConverters.put(Double.class, (Function<Number, Double>) n -> nullSafeCall(Number::doubleValue, n));
        typedConverters.put(Boolean.class, (Function<Object, Boolean>) x -> nullSafeCall(o -> o instanceof Boolean ? (Boolean)o : (o instanceof String ? Boolean.parseBoolean((String)o) : (o instanceof Number ? ((Number)o).intValue() != 0 : null)), x));
        typedConverters.put(String.class, (Function<Object, String>) n -> nullSafeCall(Object::toString, n));
        typedConverters.put(byte[].class, (Function<Object, byte[]>) x -> nullSafeCall(n -> (n instanceof byte[] ? (byte[])n : (n instanceof String ? ((String)n).getBytes() : null)), x));
        typedConverters.put(java.util.Date.class, (Function<Object, java.util.Date>) x -> nullSafeCall(n ->  (n instanceof java.util.Date ? (java.util.Date)n : (n instanceof Timestamp ? new java.util.Date(((Timestamp)n).getTime()) : null)), x));
        typedConverters.put(Date.class, (Function<Object, Date>) x -> nullSafeCall(n -> (n instanceof Date ? (Date)n : (n instanceof java.util.Date ? new Date(((java.util.Date)n).getTime()) : null)), x));
        typedConverters.put(Time.class, (Function<Object, Time>) x -> nullSafeCall(n -> (n instanceof Time ? (Time)n : (n instanceof java.util.Date ? new Time(((java.util.Date)n).getTime()) : null)), x));
        typedConverters.put(Timestamp.class, (Function<Object, Timestamp>) x -> nullSafeCall(n -> (n instanceof Timestamp ? (Timestamp)n : (n instanceof java.util.Date ? new Timestamp(((java.util.Date)n).getTime()) : null)), x));
    }


    public OrderItemsComparator(List<OrderItem> orderItems, BiFunction<T, String, ?> getter) {
        this.orderItems = orderItems;
        this.getter = getter;
    }

;
    @Override
    public int compare(T o1, T o2) {
        for (OrderItem orderItem : orderItems) {
            Object v1 = getter.apply(o1, orderItem.getName());
            Object v2 = getter.apply(o2, orderItem.getName());
            Class baseType = getClass(v1, v2);
            //noinspection unchecked
            int comparison = typedComparators.get(baseType).compare(typedConverters.get(baseType).apply(v1), typedConverters.get(baseType).apply(v2));
            if (comparison != 0) {
                return comparison * sign.get(orderItem.getDirection());
            }
        }
        return 0;
    }

    private Class<?> getClass(Object v1, Object v2) {
        if (v1 == null) {
            return v2 == null ? null : v2.getClass();
        }
        if (v2 == null) {
            return v1.getClass();
        }

        Class<?> c1 = v1.getClass();
        Class<?> c2 = v2.getClass();
        if (c1.equals(c2)) {
            return c1;
        }

        Integer n1 = typeOrder.get(c1);
        Integer n2 = typeOrder.get(c2);

        return n1 == null || n2 == null ? c1 : n1 > n2 ? c1 : c2;
    }

    private static <T, R> R nullSafeCall(Function<T, R> func, T n) {
        return n == null ? null : func.apply(n);
    }

    private static <T> int nullSafeCall(Comparator<T> comparator, T x, T y) {
        if (x == null) {
            return y == null ? 0 : -1;
        }
        if (y == null) {
            return 1;
        }
        return comparator.compare(x, y);
    }
}
