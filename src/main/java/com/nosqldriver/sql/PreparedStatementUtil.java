package com.nosqldriver.sql;

import java.util.ArrayList;
import java.util.Collection;
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

    public static Iterable<String> splitQueries(String sql) {
        Collection<String> queries = new ArrayList<>();
        StringBuilder currentQuery = new StringBuilder();
        int count = 0;
        boolean intoConstant = false;

        for (char c : sql.toCharArray()) {
            if (c == '\'') {
                intoConstant = !intoConstant;
            }
            if (!intoConstant && c == ';') {
                queries.add(currentQuery.toString());
                currentQuery.setLength(0);
            } else {
                currentQuery.append(c);
            }
        }


        if (currentQuery.length() > 0 && currentQuery.toString().trim().length() > 0) {
            String query = currentQuery.toString();
            if (query.trim().length() > 0) {
                queries.add(currentQuery.toString());
            }
        }

        return queries;
    }

}
