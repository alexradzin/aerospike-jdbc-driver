package com.nosqldriver.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.nosqldriver.sql.TypeConversion.sqlTypeNames;

//TODO: separate SimpleResultSetMetaData and SimpleResultSetMetaDataWrapper
public class SimpleResultSetMetaData implements ResultSetMetaData {
    private final ResultSetMetaData md;
    private final String schema;
    private final String[] names;
    private final String[] aliases;
    private final int[] types;


    public SimpleResultSetMetaData(ResultSetMetaData md, String schema, String[] names, String[] aliases) {
        this(md, schema, names, aliases, new int[names.length]);
    }

    public SimpleResultSetMetaData(ResultSetMetaData md, String schema, String[] names, String[] aliases, int[] types) {
        this.md = md;
        this.schema = schema;
        this.names = names;
        this.aliases = aliases;
        this.types = types;
    }


    // This method is temporary patch. Schema does not belong to metadata but to each column separately
    public String getSchema() {
        return schema;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return names.length == 0 && md != null ? md.getColumnCount() : names.length;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return false; //TODO: if indexed
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columnNullable; // any column in aerospike is nullable
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) {
        return aliases != null && aliases.length >= column && aliases[column - 1] != null ? aliases[column - 1] : getColumnName(column);
    }

    @Override
    public String getColumnName(int column) {
        return names[column - 1];
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return schema != null ? schema : md != null ? md.getSchemaName(column) : null;
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) {
        return null;
    }

    @Override
    public String getCatalogName(int column) {
        return schema;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return md != null ? md.getColumnType(column) : types != null && types.length >= column ? types[column - 1] : 0;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return sqlTypeNames.get(getColumnType(column));
    }

    @Override
    public boolean isReadOnly(int column) {
        return false;
    }

    @Override
    public boolean isWritable(int column) {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return false;
    }

    @Override
    public String getColumnClassName(int column) {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }
}
