package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.util.FunctionManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;

import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.emptyKeyRecordExtractor;
import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.keyRecordDataExtractor;
import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.keyRecordKeyExtractor;


public class ResultSetOverAerospikeRecordSet extends AerospikeRecordResultSet {
    private final RecordSet rs;
    private Iterator<KeyRecord> it;
    private KeyRecord currentRecord;

    public ResultSetOverAerospikeRecordSet(Statement statement, String schema, String table, List<DataColumn> columns, RecordSet rs, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher, FunctionManager functionManager, boolean pk) {
        super(statement, schema, table, columns,
                new GenericTypeDiscoverer<>(keyRecordsFetcher, new CompositeKeyRecordExtractor(pk ? keyRecordKeyExtractor : emptyKeyRecordExtractor, keyRecordDataExtractor), functionManager, pk),
                pk);
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
            currentRecord = it.next();
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
    protected KeyRecord getRecord() {
        return currentRecord;
    }
}
