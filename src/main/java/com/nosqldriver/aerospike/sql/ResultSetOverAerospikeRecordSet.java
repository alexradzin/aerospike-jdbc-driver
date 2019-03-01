package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.Statement;

import java.sql.SQLException;
import java.sql.SQLWarning;


public class ResultSetOverAerospikeRecordSet extends AerospikeResultSet {
    private final String schema;
    private final Statement statement;
    private final String[] names;
    private final com.aerospike.client.query.RecordSet rs;
    private boolean wasNull = false;
    private volatile SQLWarning sqlWarning;
    private volatile int index = 0;
    private volatile boolean done = false;


    public ResultSetOverAerospikeRecordSet(String schema, Statement statement, String[] names, com.aerospike.client.query.RecordSet rs) {
        super(schema, names);
        this.schema = schema;
        this.statement  = statement;
        this.names = names;
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


    // Although this method is optional and can return null accrding the spec but it is better to simulate correct behavior and return sql statement generated from Aerospike query
//    @Override
//    public java.sql.Statement getStatement() throws SQLException {
//        return null; // TODO  this class is a bridge between Aerospike statment and SQL result set???
//    }


    @Override
    protected Record getRecord() {
        return rs.getRecord();
    }
}
