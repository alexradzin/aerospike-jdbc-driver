package com.nosqldriver.sql;

import com.nosqldriver.util.CompositeComparator;
import com.nosqldriver.util.ExpressionAwareMapComparator;
import com.nosqldriver.util.PagedCollection;

import java.sql.ResultSet;
import java.util.List;
import java.util.TreeSet;

import static java.util.Comparator.comparingInt;

public class SortedResultSet extends BufferedResultSet {
    protected SortedResultSet(ResultSet rs, List<OrderItem> orderItems) {
        super(rs, new PagedCollection<>(new TreeSet<>(new CompositeComparator<>(new ExpressionAwareMapComparator(orderItems), comparingInt(System::identityHashCode))), Integer.MAX_VALUE, false, TreeSet::pollLast));
    }
}
