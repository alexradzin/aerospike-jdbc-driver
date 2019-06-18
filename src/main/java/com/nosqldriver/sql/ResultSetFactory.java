package com.nosqldriver.sql;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_INDEX;
import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_NAME;
import static com.nosqldriver.sql.ResultSetInvocationHandler.METADATA;
import static com.nosqldriver.sql.ResultSetInvocationHandler.NEXT;
import static com.nosqldriver.sql.ResultSetInvocationHandler.OTHER;

public class ResultSetFactory {
    private final ResultSetWrapperFactory wrapperFactory = new ResultSetWrapperFactory();

    public ResultSet create(String schema, String[] columns, Iterable<List<?>> data) throws SQLException {
        return create(schema, columns, discoverTypes(columns.length, data), data);
    }

    public ResultSet create(String schema, String[] columns, int[] types, Iterable<List<?>> data) throws SQLException {
        return create(schema, new SimpleResultSetMetaData(null, schema, columns, columns, types), data);
    }

    public ResultSet create(String schema, ResultSetMetaData md, Iterable<List<?>> data) throws SQLException {
        Map<String, Integer> columnIndex = IntStream.range(0, md.getColumnCount()).boxed().collect(Collectors.toMap(i -> getColumnName(md, i + 1), i -> i + 1));
        String[] columns = IntStream.range(0, md.getColumnCount()).boxed().map(i -> getColumnName(md, i + 1)).toArray(String[]::new);

        return wrapperFactory.create(new ResultSetInvocationHandler<ResultSet>(NEXT | METADATA | GET_NAME | GET_INDEX | OTHER, null, schema, columns, columns) {
            Iterator<List<?>> linesIterator = data.iterator();
            List<?> currentRow = null;

            @Override
            protected boolean next() {
                if (linesIterator.hasNext()) {
                    currentRow = linesIterator.next();
                    return true;
                }
                return false;
            }

            @Override
            protected ResultSetMetaData getMetadata() {
                return md;
            }

            @Override
            @SuppressWarnings ("unchecked")
            protected <T> T get(int columnIndex, Class<T> type) {
                return (T)currentRow.get(columnIndex - 1);
            }

            @Override
            protected <T> T get(String name, Class<T> type) {
                return get(columnIndex.get(name), type);
            }

            @Override
            protected <T> T other(Method method, Object[] args) {
                sneakyThrow(new SQLException("Unsupported operation " + method.getName()));
                return null;
            }
            @SuppressWarnings("unchecked")
            private <E extends Throwable> void sneakyThrow(Throwable e) throws E {
                throw (E) e;
            }

        });

    }

    private int[] discoverTypes(int nColumns, Iterable<List<?>> data) {
        int[] types = new int[nColumns];
        int rowIndex = 0;
        for (List<?> row : data) {
            Integer[] rowTypes = row.stream().map(v -> v == null ? null : v.getClass()).map(c -> TypeConversion.sqlTypes.getOrDefault(c, 0)).toArray(Integer[]::new);
            for (int i = 0; i < nColumns; i++) {
                int type = types[i];
                int rowType = rowTypes[i];

                if (rowType == 0) {
                    continue;
                }

                if (type == 0) {
                    types[i] = rowType;
                    continue;
                }
                if (type != rowType) {
                    throw new IllegalArgumentException(String.format("Type of value [%d,%d] %s does not match already discovered type %s", rowIndex, i, rowType, type));
                }
            }
            rowIndex++;
        }
        System.out.println("types: " + Arrays.toString(types));
        return types;
    }

    private String getColumnName(ResultSetMetaData md, int index) {
        try {
            return md.getColumnName(index);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
