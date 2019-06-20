package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;

public class ResultSetOverAerospikeRecords extends AerospikeRecordResultSet {
    private final Record[] records;
    private volatile int currentIndex = -1;

    public ResultSetOverAerospikeRecords(String schema, String[] names, Record[] records) {
        super(schema, names);
        this.records = Arrays.stream(records).filter(Objects::nonNull).toArray(Record[]::new);
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

    @Override
    protected Record getSampleRecord() {
        return currentIndex >= 0 ? records[currentIndex] : records.length > 0 ? records[0] : null;
    }
}
