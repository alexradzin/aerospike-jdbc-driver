package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.util.function.Predicate;

public class OffsetLimit implements Predicate<ResultSet> {
    private final long offset;
    private final long limit;
    private long current = 0;

    public OffsetLimit(long offset, long limit) {
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public boolean test(ResultSet resultSet) {
        current++;
        return current > offset && current - offset <= limit;
    }
}
