package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.util.SneakyThrower;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyMap;


public class ResultSetOverAerospikeRecordSet extends AerospikeRecordResultSet {

    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    private static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private final RecordSet rs;

    public ResultSetOverAerospikeRecordSet(Statement statement, String schema, String table, List<DataColumn> columns, RecordSet rs, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher) {
        super(statement, schema, table, columns, new GenericTypeDiscoverer<>(keyRecordsFetcher, keyRecordDataExtractor));
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
        try {
            return rs.getRecord();
        } catch (RuntimeException e) {
            SneakyThrower.sneakyThrow(new SQLException(e));
            return null;
        }
    }
}
