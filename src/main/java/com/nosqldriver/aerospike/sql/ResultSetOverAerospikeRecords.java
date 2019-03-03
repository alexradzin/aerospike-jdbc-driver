package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;

import java.sql.SQLException;

public class ResultSetOverAerospikeRecords extends AerospikeResultSet {
    private final Record[] records;
    private volatile int currentIndex = -1;
    private volatile boolean closed = false;
    private volatile int direction = 1;

    public ResultSetOverAerospikeRecords(String schema, String[] names, Record[] records) {
        super(schema, names);
        this.records = records;
    }


    @Override
    public boolean next() throws SQLException {
        assertClosed();
        if (currentIndex + 1 >= records.length) {
            return false;
        }
        currentIndex++;
        return true;
    }


    @Override
    protected Record getRecord() {
        return records[currentIndex];
    }
}
