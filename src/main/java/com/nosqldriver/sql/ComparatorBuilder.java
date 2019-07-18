package com.nosqldriver.sql;

import com.nosqldriver.util.CompositeComparator;
import com.nosqldriver.util.PartialComparator;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

@Deprecated
public class ComparatorBuilder<T> {
    private final List<OrderItem> orderItems = new ArrayList<>();
    private ResultSetMetaData md;
    private BiFunction<T, String, ?> getter;

    private static final Map<Class, Comparator> typedComparators = new HashMap<>();
    static {
        typedComparators.put(Integer.class, (Comparator<Integer>) (x, y) -> x == null ? y == null ? 0 : -1 : Integer.compare(x, y));
    }


    public ComparatorBuilder orderBy(OrderItem item) {
        orderItems.add(item);
        return this;
    }

    public ComparatorBuilder using(ResultSetMetaData md) {
        this.md = md;
        return this;
    }

    public ComparatorBuilder withGetter(BiFunction<T, String, ?> getter) {
        this.getter = getter;
        return this;
    }

    public Comparator<T> build() throws SQLException {
        if (md == null) {
            throw new  IllegalStateException("Cannot create  comparator without metadata");
        }

        List<Comparator<T>> comparators = new ArrayList<>();

        int n = md.getColumnCount();
        for (int i = 0; i < n; i++) {
            String label = md.getColumnLabel(i);
            Class<?> clazz = SqlLiterals.sqlToJavaTypes.get(md.getColumnType(i));

            Function<T, Object> fieldGetter = t -> getter.apply(t, label);

            //noinspection unchecked
            comparators.add(new PartialComparator<T, Object>(fieldGetter, typedComparators.get(clazz)));
        }
        //noinspection unchecked
        return new CompositeComparator<>(comparators.toArray(new Comparator[0]));
    }



}
