package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.util.CustomDeserializerManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyMap;


public class ResultSetOverAerospikeRecordSet extends AerospikeRecordResultSet {

    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    private static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private final RecordSet rs;
    private Iterator<KeyRecord> it;
    private Record currentRecord;

    public ResultSetOverAerospikeRecordSet(Statement statement, String schema, String table, List<DataColumn> columns, RecordSet rs, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher, CustomDeserializerManager cdm) {
        super(statement, schema, table, columns, new GenericTypeDiscoverer<>(keyRecordsFetcher, keyRecordDataExtractor, cdm));
        this.rs = rs;
    }

    @Override
    public boolean isLast() throws SQLException {
        if (isAfterLast()) {
            return false;
        }
        return it != null && !it.hasNext();
    }

    @Override
    protected boolean moveToNext() {
        if (it == null) {
            it = rs.iterator();
        }


        if (it.hasNext()) {
            currentRecord = it.next().record;
            return true;
        }
        return false;
    }


    @Override
    public void close() throws SQLException {
        rs.close();
        super.close();
    }

    @Override
    protected Record getRecord() {
        return currentRecord;
    }
}
