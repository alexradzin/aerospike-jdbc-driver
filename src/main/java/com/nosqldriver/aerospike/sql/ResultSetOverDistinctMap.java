package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.ResultSet;
import com.nosqldriver.sql.DataColumn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.util.Optional.ofNullable;

public class ResultSetOverDistinctMap extends ResultSetOverAerospikeResultSet {
    // Important: corresponding constant is defined in groupby.lua
    private static final String KEY_DELIMITER = "_nsqld_as_d_";

    private static final Map<String, Function<String, Object>> parsers = new HashMap<>();
    static {
        // The type names here are not Aerospike's but Lua's
        parsers.put("number", s ->
                {
                    // This code cannot be one-lined: in this case the result type is always double even if the value is int.
                    if (s.contains(".")) {
                        return parseDouble(s);
                    }
                    return parseInt(s);
                }
        );
        parsers.put("string", s -> s);
        // TODO: add support of the rest of types: bytes, list, map, GeoJson.
    }


    private Map<Object, Object> row = null;
    private List<Entry<Object, Object>> entries = null;
    private int currentIndex = -1;
    private Map<String, Object> currentRecord;
    private boolean nextWasCalled = false;
    private boolean nextResult = false;
    private Map<String, Object> sampleRecord;

    public ResultSetOverDistinctMap(String schema, String table, List<DataColumn> columns, ResultSet rs) {
        super(schema, table, columns, rs);
    }

    @Override
    public boolean next() {
        if (!nextWasCalled) {
            nextResult = rs.next();
            nextWasCalled = true;
        }
        if (!nextResult) {
            return false;
        }
        currentIndex++;
        if (row == null) {
            getRecord();
        }
        currentRecord = null;
        return currentIndex < row.size();
    }



    @Override
    protected Map<String, Object> getData(Map<String, Object> record) {
        return record;
    }


    @Override
    protected Map<String, Object> getRecord() {
        if (entries == null) {
            row = toMap(rs.getObject());
            entries = new ArrayList<>(row.entrySet());
        }

        if (currentIndex < 0) {
            return null; // This happens when attempting to bind value for script during discovery of metadata.
            //TODO: better throw exception here
        }
        Entry<Object, Object> e = entries.get(currentIndex);
        Object key = e.getKey();
        Object[] keys = Arrays.stream(key instanceof String ? ((String)key).split(KEY_DELIMITER) : new Object[] {key}).map(this::cast).toArray();

        Map<String, Object> record = new HashMap<>();
        for (int i = 0; i < keys.length; i++) {
            record.put(columns.get(i).getName(), keys[i]);
        }
        if (e.getValue() instanceof Map) { // group by
            record.putAll(toMap(e.getValue()));
        }
        currentRecord = record;

        return record;
    }

    private Object cast(Object value) {
        if (value instanceof String) {
            String key = (String)value;
            String[] tk = key.split(":", 2);
            return ofNullable(parsers.get(tk[0])).map(p -> p.apply(tk[1])).orElseThrow(() -> new IllegalStateException("Cannot identify type: " + key));
        } else {
            return value;
        }
    }

    @Override
    protected Map<String, Object> getSampleRecord() {
        if (sampleRecord != null) {
            return sampleRecord;
        }
        if (next()) {
            sampleRecord = getRecord();
            currentIndex--;
            return currentRecord;
        }

        return null;
    }

    @Override
    protected Object getValue(Map<String, Object> record, String label) {
        if(currentIndex < 0) {
            return null; //TODO: is this correct? Should exception be thrown here?
        }
        return ofNullable(getRecord()).map(r -> r.get(label)).orElse(null);
    }



    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> toMap(Object obj) {
        return (Map<K, V>)obj;
    }

}
