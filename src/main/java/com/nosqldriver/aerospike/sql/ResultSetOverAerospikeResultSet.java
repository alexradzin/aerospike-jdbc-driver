package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.ResultSet;

import java.sql.SQLException;
import java.util.Map;

import static com.nosqldriver.sql.TypeTransformer.cast;


public class ResultSetOverAerospikeResultSet extends AerospikeResultSet<Map<String, Object>> {
    private final ResultSet rs;
    private int index = 0;
    //private boolean done = false;
    private boolean firstNextWasCalled = false;


    public ResultSetOverAerospikeResultSet(String schema, String[] names, ResultSet rs) {
        super(schema, names);
        this.rs = rs;
    }



    @Override
    public void close() throws SQLException {
        rs.close();
        super.close();
    }


    // Although this method is optional and can return null according the spec but it is better to simulate correct behavior and return sql statement generated from Aerospike query
//    @Override
//    public java.sql.Statement getStatement() throws SQLException {
//        return null;
//    }


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


    @Override
    public boolean next() throws SQLException {
        if (firstNextWasCalled && index == 1) {
            firstNextWasCalled = false;
            return true;
        }
        boolean result = rs.next();
        if (result) {
            index++;
        } else {
            done = true;
        }
        return result;
    }

    @Override
    protected Map<String, Object> getSampleRecord() {
        if (index > 0) {
            return getRecord();
        }

        try {
            if (next()) {
                firstNextWasCalled = true;
                return getRecord();
            }
        } catch (SQLException e) {
            return null;
        }

        return null;
    }

}
