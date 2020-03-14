package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.util.FunctionManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

import static java.util.Collections.emptyMap;

public class ResultSetOverAerospikeScan extends BaseSchemalessResultSet<KeyRecord> {
    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    private static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private final ScanCallback callback;
    private volatile KeyRecord current;
    private final BlockingQueue<KeyRecord> queue = new ArrayBlockingQueue<>(10);
    private static final KeyRecord barrier = new KeyRecord(new Key("done", "done", "done"), new Record(emptyMap(), 0, 0));

    public ResultSetOverAerospikeScan(IAerospikeClient client, Statement statement, String schema, String table, List<DataColumn> columns, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher, FunctionManager functionManager) {
        super(statement, schema, table, columns, new GenericTypeDiscoverer<>(keyRecordsFetcher, keyRecordDataExtractor, functionManager));
        this.callback = (key, record) -> enqueue(new KeyRecord(key, record));

        new Thread(() -> {
            client.scanAll(new ScanPolicy(), schema, table, callback);
            enqueue(barrier);
        }).start();
    }

    @Override
    protected KeyRecord getRecord() {
        return current;
    }

    @Override
    protected boolean moveToNext() {
        try {
            current = queue.take();
            return current != barrier;
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected Object getValue(KeyRecord record, String label) {
        return "PK".equals(label) ? record.key : record.record.getValue(label);
    }

    @Override
    protected String getString(KeyRecord record, String label) {
        return record.record.getString(label);
    }

    @Override
    protected boolean getBoolean(KeyRecord record, String label) {
        return record.record.getBoolean(label);
    }

    @Override
    protected byte getByte(KeyRecord record, String label) {
        return record.record.getByte(label);
    }

    @Override
    protected short getShort(KeyRecord record, String label) {
        //This line should work here. But there is a strange bug: driver assumes that all number are returned as long, so
        //getShort() calls (short)getLong(). When value is short it is inded short and getLong() fails on casting because
        //it tries to cast Short to Long first. So, I have to implement the following workaround here.
        //return record.record.getShort(label);
        return (short)record.record.getValue(label);
    }

    @Override
    protected int getInt(KeyRecord record, String label) {
        return record.record.getInt(label);
    }

    @Override
    protected long getLong(KeyRecord record, String label) {
        return record.record.getLong(label);
    }

    @Override
    protected float getFloat(KeyRecord record, String label) {
        return record.record.getFloat(label);
    }

    @Override
    protected double getDouble(KeyRecord record, String label) {
        return record.record.getDouble(label);
    }

    @Override
    public void close() throws SQLException {
        super.close();
        enqueue(barrier);
    }

    private void enqueue(KeyRecord record) {
        try {
            queue.put(record);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }
}
