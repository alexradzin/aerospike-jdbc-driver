package com.nosqldriver.aerospike.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class AerospikeResultSetMetaData implements ResultSetMetaData {
    private final ResultSetMetaData md;
    private final String schema;
    private final String[] names;
    private final String[] aliases;

    public AerospikeResultSetMetaData(ResultSetMetaData md, String schema, String[] names, String[] aliases) {
        this.md = md;
        this.schema = schema;
        this.names = names;
        this.aliases = aliases;
    }


    @Override
    public int getColumnCount() throws SQLException {
        return names.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false; //TODO: if indexed
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable; // any column in aerospike is nullable
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return aliases!= null && aliases.length >= column && aliases[column - 1] != null ? aliases[column - 1] : getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return names[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return schema != null ? schema : md != null ? md.getSchemaName(column) : null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
