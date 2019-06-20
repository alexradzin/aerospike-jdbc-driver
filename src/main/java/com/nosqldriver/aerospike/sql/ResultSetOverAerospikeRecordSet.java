package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;

import java.sql.SQLException;


public class ResultSetOverAerospikeRecordSet extends AerospikeRecordResultSet {
    private final RecordSet rs;
    private int index = 0;
    private boolean done = false;
    private boolean firstNextWasCalled = false;


    public ResultSetOverAerospikeRecordSet(String schema, String[] names, RecordSet rs) {
        super(schema, names);
        this.rs = rs;
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
    protected Record getSampleRecord() {
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


    @Override
    protected Record getRecord() {
        return rs.getRecord();
    }


}
