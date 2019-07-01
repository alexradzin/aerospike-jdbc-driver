package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.ResultSet;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import static com.nosqldriver.sql.TypeTransformer.cast;


public class ResultSetOverAerospikeResultSet extends BaseSchemalessResultSet<Map<String, Object>> {
    protected final ResultSet rs;
    public ResultSetOverAerospikeResultSet(String schema, String[] names, List<DataColumn> columns, ResultSet rs) {
        super(schema, names, columns);
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
    protected String getString(Map<String, Object> record, String label) {
        return cast(getValue(record, label), String.class);
    }

    @Override
    protected boolean getBoolean(Map<String, Object> record, String label) {
        return cast(getValue(record, label), Boolean.class);
    }

    @Override
    protected byte getByte(Map<String, Object> record, String label) {
        return cast(getValue(record, label), byte.class);
    }

    @Override
    protected short getShort(Map<String, Object> record, String label) {
        return cast(getValue(record, label), short.class);
    }

    @Override
    protected int getInt(Map<String, Object> record, String label) {
        return cast(getValue(record, label), int.class);
    }

    @Override
    protected long getLong(Map<String, Object> record, String label) {
        return cast(getValue(record, label), long.class);
    }

    @Override
    protected float getFloat(Map<String, Object> record, String label) {
        return cast(getValue(record, label), float.class);
    }

    @Override
    protected double getDouble(Map<String, Object> record, String label) {
        return cast(getValue(record, label), double.class);
    }

    protected boolean moveToNext() {
        return rs.next();
    }
}
