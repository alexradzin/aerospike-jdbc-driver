package com.nosqldriver.util;

import com.nosqldriver.sql.OrderItem;
import com.nosqldriver.sql.OrderItemsComparator;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ExpressionAwareComparator<T> extends OrderItemsComparator<T> {
    public ExpressionAwareComparator(List<OrderItem> orderItems, BiFunction<T, String, Object> propGetter, Function<T, Iterable<String>> nameLister) {
        super(orderItems, new ExpressionAwarePropertyGetter<>(propGetter, nameLister));
    }
}
