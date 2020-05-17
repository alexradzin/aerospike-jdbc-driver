package com.nosqldriver.sql;

import com.nosqldriver.util.CompositeComparator;
import com.nosqldriver.util.ExpressionAwareMapComparator;
import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.PagedCollection;

import java.sql.ResultSet;
import java.util.List;
import java.util.TreeSet;

import static java.lang.String.format;
import static java.util.Comparator.comparingInt;

public class SortedResultSet extends BufferedResultSet {
    private final DriverPolicy driverPolicy;
    public SortedResultSet(ResultSet rs, List<OrderItem> orderItems, FunctionManager functionManager, DriverPolicy driverPolicy) {
        this(rs, orderItems, Integer.MAX_VALUE, functionManager, driverPolicy);
    }

    public SortedResultSet(ResultSet rs, List<OrderItem> orderItems, long limit, FunctionManager functionManager, DriverPolicy driverPolicy) {
        //limit is long here because limit and offset returned by SQL parser are long. However fetchSize of JDBC is int, so we have to cast limit to int.
        super(rs, new PagedCollection<>(new TreeSet<>(new CompositeComparator<>(new ExpressionAwareMapComparator(orderItems, functionManager, driverPolicy), comparingInt(System::identityHashCode))), limit, false, TreeSet::pollLast), safeCast(limit));
        this.driverPolicy = driverPolicy;
    }

    private static int safeCast(long l) {
        if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
            throw new IllegalArgumentException(format("Cannot cast value %d to int", l));
        }
        return (int)l;
    }

}
