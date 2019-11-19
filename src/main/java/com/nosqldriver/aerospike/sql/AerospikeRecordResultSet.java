package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.TypeDiscoverer;

import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

abstract class AerospikeRecordResultSet extends BaseSchemalessResultSet<Record> {
    protected AerospikeRecordResultSet(Statement statement, String schema, String table, List<DataColumn> columns, TypeDiscoverer typeDiscoverer) {
        super(statement, schema, table, columns, typeDiscoverer);
    }

    @Override
    protected abstract Record getRecord();

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
        return ((Number)record.getValue(label)).byteValue();
    }

    @Override
    protected short getShort(Record record, String label) {
        return ((Number) record.getValue(label)).shortValue(); // using getValue instead of getShort as a workaround over the bug in AS client.
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
