package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class ResultSetOverAerospikeRecords extends AerospikeRecordResultSet {
    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    private static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private final Record[] records;
    private int currentIndex = -1;

    public ResultSetOverAerospikeRecords(Statement statement, String schema, String table, List<DataColumn> columns, Record[] records, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher) {
        super(statement, schema, table, columns, Arrays.stream(records).anyMatch(Objects::nonNull) ? new GenericTypeDiscoverer<>((c, t) -> Arrays.asList(records), recordDataExtractor) : new GenericTypeDiscoverer<>(keyRecordsFetcher, keyRecordDataExtractor));
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

    // This method just throws exception. It is not implemented here since this class implements getSampleRecord() and next() itself without using the base calss' implementation
    protected boolean moveToNext() {
        throw new IllegalStateException("This method is not expected to be called here. ");
    }
}
