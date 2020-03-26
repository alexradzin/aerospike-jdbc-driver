package com.nosqldriver.sql;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.nosqldriver.sql.TypeTransformer.cast;

abstract class ValueTypedResultSet<R> extends BaseSchemalessResultSet<R> {
    protected ValueTypedResultSet(Statement statement, String schema, String table, List<DataColumn> columns, TypeDiscoverer typeDiscoverer) {
        super(statement, schema, table, columns, typeDiscoverer, false);
    }

    @Override
    protected String getString(R record, String label) throws SQLException {
        return cast(getValue(record, label), String.class);
    }

    @Override
    protected boolean getBoolean(R record, String label) throws SQLException {
        return cast(getValue(record, label), Boolean.class);
    }

    @Override
    protected byte getByte(R record, String label) throws SQLException {
        return cast(getValue(record, label), byte.class);
    }

    @Override
    protected short getShort(R record, String label) throws SQLException {
        return cast(getValue(record, label), short.class);
    }

    @Override
    protected int getInt(R record, String label) throws SQLException {
        return cast(getValue(record, label), int.class);
    }

    @Override
    protected long getLong(R record, String label) throws SQLException {
        return cast(getValue(record, label), long.class);
    }

    @Override
    protected float getFloat(R record, String label) throws SQLException {
        return cast(getValue(record, label), float.class);
    }

    @Override
    protected double getDouble(R record, String label) throws SQLException {
        return cast(getValue(record, label), double.class);
    }
}
