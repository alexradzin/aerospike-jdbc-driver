package com.nosqldriver.util;

import java.util.Comparator;

public class CompositeComparator<T> implements Comparator<T> {
    private final Comparator<T>[] comparators;

    @SafeVarargs
    public CompositeComparator(Comparator<T> ... comparators) {
        this.comparators = comparators;
    }

    @Override
    public int compare(T o1, T o2) {
        for (int i = 0; i < comparators.length; i++) {
            int comparison = comparators[i].compare(o1, o2);
            if (comparison != 0) {
                return comparison;
            }
        }
        return 0;
    }
}
