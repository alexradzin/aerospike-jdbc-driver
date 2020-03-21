package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.SneakyThrower;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ResultSetRowFilter extends ExpressionEvaluator<ResultSet> {
    public ResultSetRowFilter(String expr, FunctionManager functionManager) {
        super(expr, Collections.emptyMap(), functionManager);
    }

    @Override
    protected Map<String, Object> toMap(ResultSet rs) {
        return SneakyThrower.get(() -> {
            Map<String, Object> ctx = new HashMap<>();
            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();
            for (int i = 1; i <= n; i++) {
                String name = md.getColumnName(i);
                ctx.put(name, rs.getObject(i));
                String label = md.getColumnLabel(i);
                if (label != null) {
                    ctx.put(label, rs.getObject(i));
                }
            }
            return ctx;
        });
    }
}
