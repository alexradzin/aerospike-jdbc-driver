package com.nosqldriver.sql;

import java.util.List;
import java.util.function.Supplier;

import static com.nosqldriver.sql.TypeTransformer.cast;

abstract class ValueTypedResultSet<R> extends BaseSchemalessResultSet<R> {
    protected ValueTypedResultSet(String schema, String table, List<DataColumn> columns, Supplier<R> anyRecordSupplier) {
        super(schema, table, columns, anyRecordSupplier);
    }

    @Override
    protected String getString(R record, String label) {
        return cast(getValue(record, label), String.class);
    }

    @Override
    protected boolean getBoolean(R record, String label) {
        return cast(getValue(record, label), Boolean.class);
    }

    @Override
    protected byte getByte(R record, String label) {
        return cast(getValue(record, label), byte.class);
    }

    @Override
    protected short getShort(R record, String label) {
        return cast(getValue(record, label), short.class);
    }

    @Override
    protected int getInt(R record, String label) {
        return cast(getValue(record, label), int.class);
    }

    @Override
    protected long getLong(R record, String label) {
        return cast(getValue(record, label), long.class);
    }

    @Override
    protected float getFloat(R record, String label) {
        return cast(getValue(record, label), float.class);
    }

    @Override
    protected double getDouble(R record, String label) {
        return cast(getValue(record, label), double.class);
    }
}
