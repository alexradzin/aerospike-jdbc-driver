package com.nosqldriver.sql;

import java.util.Collections;
import java.util.Map.Entry;

public class PreparedStatementUtil {
    public static Entry<String, Integer> parseParameters(String sql, int offset) {
        StringBuilder fixedIndexSqlBuf = new StringBuilder();
        int count = 0;
        boolean intoConstant = false;
        for (char c : sql.toCharArray()) {
            if (c == '\'') {
                intoConstant = !intoConstant;
            }
            if (!intoConstant && c == '?') {
                count++;
                fixedIndexSqlBuf.append('$').append(count + offset);
            } else {
                fixedIndexSqlBuf.append(c);
            }
        }
        return Collections.singletonMap(fixedIndexSqlBuf.toString(), count).entrySet().iterator().next();
    }
}
