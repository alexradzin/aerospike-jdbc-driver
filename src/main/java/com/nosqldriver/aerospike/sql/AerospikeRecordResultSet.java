package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.TypeDiscoverer;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static com.nosqldriver.sql.TypeTransformer.cast;

abstract class AerospikeRecordResultSet extends BaseSchemalessResultSet<Record> {
    protected AerospikeRecordResultSet(Statement statement, String schema, String table, List<DataColumn> columns, TypeDiscoverer typeDiscoverer) {
        super(statement, schema, table, columns, typeDiscoverer);
    }

    @Override
    protected abstract Record getRecord();

    @Override
    protected Object getValue(Record record, String label) {
        return record.getValue(label);
    }

    @Override
    protected String getString(Record record, String label) {
        return record.getString(label);
    }

    @Override
    protected boolean getBoolean(Record record, String label) throws SQLException {
        return cast(record.getValue(label), boolean.class);
    }

    @Override
    protected byte getByte(Record record, String label) throws SQLException {
        return cast(record.getValue(label), byte.class);
    }

    @Override
    protected short getShort(Record record, String label) throws SQLException {
        return cast(record.getValue(label), short.class);
    }

    @Override
    protected int getInt(Record record, String label) throws SQLException {
        return cast(record.getValue(label), int.class);
    }

    @Override
    protected long getLong(Record record, String label) throws SQLException {
        return cast(record.getValue(label), long.class);
    }

    @Override
    protected float getFloat(Record record, String label) throws SQLException {
        return cast(record.getValue(label), float.class);
    }

    @Override
    protected double getDouble(Record record, String label) throws SQLException {
        return cast(record.getValue(label), double.class);
    }
}
