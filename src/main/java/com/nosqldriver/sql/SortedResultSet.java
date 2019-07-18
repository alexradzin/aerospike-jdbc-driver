package com.nosqldriver.sql;

import com.nosqldriver.util.CompositeComparator;
import com.nosqldriver.util.ExpressionAwareMapComparator;
import com.nosqldriver.util.PagedCollection;

import java.sql.ResultSet;
import java.util.List;
import java.util.TreeSet;

import static java.util.Comparator.comparingInt;

public class SortedResultSet extends BufferedResultSet {
    public SortedResultSet(ResultSet rs, List<OrderItem> orderItems) {
        this(rs, orderItems, Integer.MAX_VALUE);
    }

    public SortedResultSet(ResultSet rs, List<OrderItem> orderItems, long limit) {
        super(rs, new PagedCollection<>(new TreeSet<>(new CompositeComparator<>(new ExpressionAwareMapComparator(orderItems), comparingInt(System::identityHashCode))), limit, false, TreeSet::pollLast));
    }
}
