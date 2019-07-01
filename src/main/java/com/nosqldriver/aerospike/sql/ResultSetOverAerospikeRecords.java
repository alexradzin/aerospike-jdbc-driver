package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class ResultSetOverAerospikeRecords extends AerospikeRecordResultSet {
    private final Record[] records;
    private int currentIndex = -1;

    public ResultSetOverAerospikeRecords(String schema, String[] names, List<DataColumn> columns, Record[] records) {
        super(schema, names, columns);
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

    // This method just thows exception. It is not implemented here since this class implements getSampleRecord() and next() itself without using the base calss' implementation
    protected boolean moveToNext() {
        throw new IllegalStateException("This method is not expected to be called here. ");
    }

}
