package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Supplier;


public class ResultSetOverAerospikeRecordSet extends AerospikeRecordResultSet {
    private final RecordSet rs;

    public ResultSetOverAerospikeRecordSet(Statement statement, String schema, String table, List<DataColumn> columns, RecordSet rs, Supplier<Record> anyRecordSupplier) {
        super(statement, schema, table, columns, anyRecordSupplier);
        this.rs = rs;
    }


    @Override
    protected boolean moveToNext() {
        return rs.next();
    }


    @Override
    public void close() throws SQLException {
        rs.close();
        super.close();
    }

    @Override
    protected Record getRecord() {
        return rs.getRecord();
    }
}
