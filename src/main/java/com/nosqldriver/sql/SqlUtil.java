package com.nosqldriver.sql;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class for processing of SQL queries
 */
public class SqlUtil {
    private static final Pattern selectPattern = Pattern.compile("^select\\s+\\*\\s+from\\s+((:?\\w|-|\\.|\")+)", Pattern.CASE_INSENSITIVE);

    /**
     * Some tools in some cases generate syntactically wrong queries. For example when namespace or set contain dashes
     * the query generated for getting the table metadata does not quote identifiers: {@code select * from the-namespace.the-table}.
     * This query is incorrect and cannot be parsed. This utility wraps with quotes namespace and table names for
     * this specific query.
     * @param sql given SQL query
     * @return SQL query with quoted identifiers.
     */
    public static String fix(String sql) {
        Matcher m = selectPattern.matcher(sql);
        if (m.find()) {
            String target = m.group(1);
            String wrappedTarget = Arrays.stream(target.split("\\.")).map(SqlUtil::wrap).collect(Collectors.joining("."));
            return sql.substring(0, m.start(1)) + wrappedTarget + sql.substring(m.end(1));
        }
        return sql;
    }

    private static String wrap(String s) {
        return s.startsWith("\"") && s.endsWith("\"") ? s : "\"" + s + "\"";
    }
}
