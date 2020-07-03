package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.util.FunctionManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.emptyKeyRecordExtractor;
import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.keyRecordDataExtractor;
import static com.nosqldriver.aerospike.sql.KeyRecordFetcherFactory.keyRecordKeyExtractor;
import static com.nosqldriver.aerospike.sql.SpecialField.PK;
import static java.util.Collections.emptyMap;
import static java.util.Optional.ofNullable;

public class ResultSetOverAerospikeScan extends BaseSchemalessResultSet<KeyRecord> {
    private final ScanCallback callback;
    private volatile KeyRecord current;
    private final BlockingQueue<KeyRecord> queue = new ArrayBlockingQueue<>(10);
    private static final KeyRecord barrier = new KeyRecord(new Key("done", "done", "done"), new Record(emptyMap(), 0, 0));

    public ResultSetOverAerospikeScan(IAerospikeClient client, Statement statement, String schema, String table, List<DataColumn> columns, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher, FunctionManager functionManager, Collection<SpecialField> specialFields) {
        super(statement,
                schema,
                table,
                columns,
                new GenericTypeDiscoverer<>(keyRecordsFetcher, new CompositeKeyRecordExtractor(specialFields.contains(PK) ? keyRecordKeyExtractor : emptyKeyRecordExtractor, keyRecordDataExtractor), functionManager, specialFields),
                specialFields);
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
        return "PK".equals(label) ? specialFields.contains(PK) ? ofNullable(record.key.userKey).map(Value::getObject).orElse(null) : record.key : record.record.getValue(label);
    }

    private <T> T getValue(KeyRecord record, String label, Function<Value, T> keyGetter, BiFunction<Record, String, T> valueGetter) {
        return "PK".equals(label) ? ofNullable(record.key.userKey).map(keyGetter).orElse(null) : valueGetter.apply(record.record, label);
    }

    @Override
    protected String getString(KeyRecord record, String label) {
        return getValue(record, label, value -> (String) value.getObject(), Record::getString);
    }

    @Override
    protected boolean getBoolean(KeyRecord record, String label) {
        return getValue(record, label, value -> (Boolean) value.getObject(), Record::getBoolean);
    }

    @Override
    protected byte getByte(KeyRecord record, String label) {
        return getValue(record, label, value -> (byte) value.toInteger(), Record::getByte);
    }

    @Override
    protected short getShort(KeyRecord record, String label) {
        //This line should work here. But there is a strange bug: driver assumes that all number are returned as long, so
        //getShort() calls (short)getLong(). When value is short it is inded short and getLong() fails on casting because
        //it tries to cast Short to Long first. So, I have to implement the following workaround here.
        //return record.record.getShort(label);
        return getValue(record, label, value -> (short) value.toInteger(), (record1, s) -> (short) record1.getValue(label));
    }

    @Override
    protected int getInt(KeyRecord record, String label) {
        return getValue(record, label, Value::toInteger, Record::getInt);
    }

    @Override
    protected long getLong(KeyRecord record, String label) {
        return getValue(record, label, Value::toLong, Record::getLong);
    }

    @Override
    protected float getFloat(KeyRecord record, String label) {
        return getValue(record, label, value -> (Float) value.getObject(), Record::getFloat);
    }

    @Override
    protected double getDouble(KeyRecord record, String label) {
        return getValue(record, label, value -> (Double) value.getObject(), Record::getDouble);
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
