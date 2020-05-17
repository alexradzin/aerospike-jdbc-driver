package com.nosqldriver.util;

import com.nosqldriver.sql.DriverPolicy;
import com.nosqldriver.sql.OrderItem;

import java.util.List;
import java.util.Map;

public class ExpressionAwareMapComparator extends ExpressionAwareComparator<Map<String, Object>> {
    public ExpressionAwareMapComparator(List<OrderItem> orderItems, FunctionManager functionManager, DriverPolicy driverPolicy) {
        super(orderItems, Map::get, Map::keySet, functionManager, driverPolicy);
    }
}
