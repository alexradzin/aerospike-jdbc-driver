package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;

import java.sql.SQLException;


public class ResultSetOverAerospikeRecordSet extends AerospikeResultSet {
    private final RecordSet rs;
    private volatile int index = 0;
    private volatile boolean done = false;


    public ResultSetOverAerospikeRecordSet(String schema, String[] names, RecordSet rs) {
        super(schema, names);
        this.rs = rs;
    }


    @Override
    public boolean next() throws SQLException {
        boolean result = rs.next();
        if (result) {
            index++;
        } else {
            done = true;
        }
        return result;
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
