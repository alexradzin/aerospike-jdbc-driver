package com.nosqldriver.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

public class CompositeResultSetMetaData implements ResultSetMetaData {
    private final Collection<ResultSetMetaData> metadataElements;

    public CompositeResultSetMetaData(Collection<ResultSetMetaData> metadataElements) {
        this.metadataElements = metadataElements;
    }

    @Override
    public int getColumnCount() throws SQLException {
        int count = 0;
        for (ResultSetMetaData md : metadataElements) {
            count += md.getColumnCount();
        }
        return count;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isAutoIncrement(entry.getValue());
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isCaseSensitive(entry.getValue());
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isSearchable(entry.getValue());
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isCurrency(entry.getValue());
    }

    @Override
    public int isNullable(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isNullable(entry.getValue());
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isSigned(entry.getValue());
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getColumnDisplaySize(entry.getValue());
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getColumnLabel(entry.getValue());
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getColumnName(entry.getValue());
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        try {
            Entry<ResultSetMetaData, Integer> entry = find(column);
            return entry.getKey().getSchemaName(entry.getValue());
        } catch (IllegalStateException e) {
            return metadataElements.isEmpty() ? null : metadataElements.iterator().next().getSchemaName(column);
        }
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getPrecision(entry.getValue());
    }

    @Override
    public int getScale(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getScale(entry.getValue());
    }

    @Override
    public String getTableName(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getTableName(entry.getValue());
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getCatalogName(entry.getValue());
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getColumnType(entry.getValue());
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getColumnTypeName(entry.getValue());
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isReadOnly(entry.getValue());
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isWritable(entry.getValue());
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().isDefinitelyWritable(entry.getValue());
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        Entry<ResultSetMetaData, Integer> entry = find(column);
        return entry.getKey().getColumnTypeName(entry.getValue());
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private Entry<ResultSetMetaData, Integer> find(int absoluteColumn) throws SQLException {
        int relativeColumn = absoluteColumn;
        for (ResultSetMetaData md : metadataElements) {
            int count = md.getColumnCount();
            if (relativeColumn <= count) {
                return Collections.singletonMap(md, relativeColumn).entrySet().iterator().next();
            }
            relativeColumn -= count;
        }

        throw new IllegalStateException("Cannot find column " + absoluteColumn);
    }
}
