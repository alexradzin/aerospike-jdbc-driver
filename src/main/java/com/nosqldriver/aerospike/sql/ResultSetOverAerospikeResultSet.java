package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.ResultSet;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static com.nosqldriver.sql.TypeTransformer.cast;


public class ResultSetOverAerospikeResultSet extends BaseSchemalessResultSet<Map<String, Object>> {
    protected final ResultSet rs;
    public ResultSetOverAerospikeResultSet(Statement statement, String schema, String table, List<DataColumn> columns, ResultSet rs, Supplier<Map<String, Object>> anyRecordSupplier) {
        super(statement, schema, table, columns, anyRecordSupplier);
        this.rs = rs;
    }



    @Override
    public void close() throws SQLException {
        rs.close();
        super.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Map<String, Object> getRecord() {
        return (Map<String, Object>)rs.getObject();
    }

    @Override
    protected Map<String, Object> getData(Map<String, Object> record) {
        return getRecord();
    }

    @Override
    protected Object getValue(Map<String, Object> record, String label) {
        return record.get(label);
    }

    @Override
    protected String getString(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), String.class);
    }

    @Override
    protected boolean getBoolean(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), Boolean.class);
    }

    @Override
    protected byte getByte(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), byte.class);
    }

    @Override
    protected short getShort(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), short.class);
    }

    @Override
    protected int getInt(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), int.class);
    }

    @Override
    protected long getLong(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), long.class);
    }

    @Override
    protected float getFloat(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), float.class);
    }

    @Override
    protected double getDouble(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), double.class);
    }

    protected boolean moveToNext() {
        return rs.next();
    }
}
