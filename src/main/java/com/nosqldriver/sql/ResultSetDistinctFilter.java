package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

public class ResultSetDistinctFilter<T> implements Predicate<ResultSet> {
    private final Function<ResultSet, T> valueExtractor;
    private final Collection<T> values;


    public ResultSetDistinctFilter(Function<ResultSet, T> valueExtractor, Collection<T> values) {
        this.valueExtractor = valueExtractor;
        this.values = values;
    }


    @Override
    public boolean test(ResultSet rs) {
        return values.add(valueExtractor.apply(rs));
    }
}
