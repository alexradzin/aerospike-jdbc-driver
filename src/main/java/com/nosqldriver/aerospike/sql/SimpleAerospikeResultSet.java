package com.nosqldriver.aerospike.sql;

import static com.nosqldriver.sql.TypeTransformer.cast;

abstract class SimpleAerospikeResultSet<R> extends AerospikeResultSet<R> {
    protected SimpleAerospikeResultSet(String schema, String[] names) {
        super(schema, names);
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
