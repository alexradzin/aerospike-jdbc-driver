package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.RecordSet;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.util.List;


public class ResultSetOverAerospikeRecordSet extends AerospikeRecordResultSet {
    private final RecordSet rs;

    public ResultSetOverAerospikeRecordSet(String schema, List<DataColumn> columns, RecordSet rs) {
        super(schema, columns);
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
