package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.util.FunctionManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.emptyKeyRecordExtractor;
import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.keyRecordDataExtractor;
import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.keyRecordKeyExtractor;
import static com.nosqldriver.aerospike.sql.SpecialField.PK;
import static java.util.Arrays.asList;

public class ResultSetOverAerospikeRecords extends AerospikeRecordResultSet {
    private final KeyRecord[] records;
    private int currentIndex = -1;

    public ResultSetOverAerospikeRecords(Statement statement, String schema, String table, List<DataColumn> columns, KeyRecord[] records, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher, FunctionManager functionManager, Collection<SpecialField> specialFields) {
        super(
                statement,
                schema,
                table,
                columns,
                Arrays.stream(records).anyMatch(record -> record.record != null) ?
                        new GenericTypeDiscoverer<>((c, t) -> asList(records), new CompositeKeyRecordExtractor(KeyRecordFetcherFactory.extractors(specialFields)), functionManager, specialFields) :
                        new GenericTypeDiscoverer<>(keyRecordsFetcher, new CompositeKeyRecordExtractor(specialFields.contains(PK) ? keyRecordKeyExtractor : emptyKeyRecordExtractor, keyRecordDataExtractor), functionManager, specialFields),

                specialFields);
        this.records = Arrays.stream(records).filter(record -> record.record != null).toArray(KeyRecord[]::new);
    }


    @Override
    public boolean next() throws SQLException {
        assertClosed();
        if (currentIndex + 1 >= records.length) {
            afterLast = true;
            return false;
        }
        currentIndex++;
        index++;
        return true;
    }


    @Override
    public boolean isLast() throws SQLException {
        return !isAfterLast() && (currentIndex == records.length - 1 && records.length > 0);
    }


    @Override
    protected KeyRecord getRecord() {
        return records.length > 0 ? records[currentIndex] : null;
    }

    // This method just throws exception. It is not implemented here since this class implements getSampleRecord() and next() itself without using the base calss' implementation
    protected boolean moveToNext() {
        throw new IllegalStateException("This method is not expected to be called here. ");
    }
}
