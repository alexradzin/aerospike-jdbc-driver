package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;

import java.sql.SQLException;
import java.util.Map;

abstract class AerospikeRecordResultSet extends AerospikeResultSet<Record> {
    protected AerospikeRecordResultSet(String schema, String[] names) {
        super(schema, names);
    }


    @Override
    public abstract boolean next() throws SQLException;

    @Override
    protected abstract Record getRecord();

    @Override
    protected abstract Record getSampleRecord();

    @Override
    protected Map<String, Object> getData(Record record) {
        return record.bins;
    }

    @Override
    protected Object getValue(Record record, String label) {
        return record.getValue(label);
    }

    @Override
    protected String getString(Record record, String label) {
        return record.getString(label);
    }

    @Override
    protected boolean getBoolean(Record record, String label) {
        return record.getBoolean(label);
    }

    @Override
    protected byte getByte(Record record, String label) {
        return record.getByte(label);
    }

    @Override
    protected short getShort(Record record, String label) {
        return record.getShort(label);
    }

    @Override
    protected int getInt(Record record, String label) {
        return record.getInt(label);
    }

    @Override
    protected long getLong(Record record, String label) {
        return record.getLong(label);
    }

    @Override
    protected float getFloat(Record record, String label) {
        return record.getFloat(label);
    }

    @Override
    protected double getDouble(Record record, String label) {
        return record.getDouble(label);
    }
}
