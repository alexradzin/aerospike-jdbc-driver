package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.TypeDiscoverer;
import com.nosqldriver.util.ValueExtractor;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.nosqldriver.sql.TypeTransformer.cast;

abstract class AerospikeRecordResultSet extends BaseSchemalessResultSet<Record> {
    private final ValueExtractor valueExtractor = new ValueExtractor();

    protected AerospikeRecordResultSet(Statement statement, String schema, String table, List<DataColumn> columns, TypeDiscoverer typeDiscoverer) {
        super(statement, schema, table, columns, typeDiscoverer);
    }

    @Override
    protected abstract Record getRecord();

    @Override
    protected Object getValue(Record record, String label) {
        return valueExtractor.getValue(toMap(record), label);
    }

    @Override
    protected String getString(Record record, String label) throws SQLException {
        return getTypedValue(record, label, String.class);
    }

    @Override
    protected boolean getBoolean(Record record, String label) throws SQLException {
        return getTypedValue(record, label, boolean.class);
    }

    @Override
    protected byte getByte(Record record, String label) throws SQLException {
        return getTypedValue(record, label, byte.class);
    }

    @Override
    protected short getShort(Record record, String label) throws SQLException {
        return getTypedValue(record, label, short.class);
    }

    @Override
    protected int getInt(Record record, String label) throws SQLException {
        return getTypedValue(record, label, int.class);
    }

    @Override
    protected long getLong(Record record, String label) throws SQLException {
        return getTypedValue(record, label, long.class);
    }

    @Override
    protected float getFloat(Record record, String label) throws SQLException {
        return getTypedValue(record, label, float.class);
    }

    @Override
    protected double getDouble(Record record, String label) throws SQLException {
        return getTypedValue(record, label, double.class);
    }

    private <T> T getTypedValue(Record record, String label, Class<T> clazz) throws SQLException {
        return cast(valueExtractor.getValue(toMap(record), label), clazz);
    }

    private Map<String, Object> toMap(Record record) {
        return record.bins == null ? Collections.emptyMap() : record.bins;
    }
}
