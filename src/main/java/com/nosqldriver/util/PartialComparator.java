package com.nosqldriver.util;

import java.util.Comparator;
import java.util.function.Function;

public class PartialComparator<T, V> implements Comparator<T> {
    private final Function<T, V> getter;
    private final Comparator<V> partComparator;

    public PartialComparator(Function<T, V> getter, Comparator<V> partComparator) {
        this.getter = getter;
        this.partComparator = partComparator;
    }

    @Override
    public int compare(T o1, T o2) {
        return partComparator.compare(getter.apply(o1), getter.apply(o2));
    }
}
