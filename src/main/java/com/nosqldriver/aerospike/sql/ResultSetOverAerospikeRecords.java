package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class ResultSetOverAerospikeRecords extends AerospikeRecordResultSet {
    private final Record[] records;
    private final Supplier<Record> anyRecordSupplier;
    private int currentIndex = -1;

    public ResultSetOverAerospikeRecords(Statement statement, String schema, String table, List<DataColumn> columns, Record[] records, Supplier<Record> anyRecordSupplier) {
        super(statement, schema, table, columns, anyRecordSupplier);
        this.records = Arrays.stream(records).filter(Objects::nonNull).toArray(Record[]::new);
        this.anyRecordSupplier = anyRecordSupplier;
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
        return currentIndex >= 0 ? records[currentIndex] : records.length > 0 ? records[0] : anyRecordSupplier.get();
    }

    // This method just thows exception. It is not implemented here since this class implements getSampleRecord() and next() itself without using the base calss' implementation
    protected boolean moveToNext() {
        throw new IllegalStateException("This method is not expected to be called here. ");
    }

}
