package com.nosqldriver.aerospike.sql;

import com.nosqldriver.sql.SimpleResultSetMetaData;
import com.nosqldriver.sql.TypeConversion;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ListRecordSet extends SimpleAerospikeResultSet<List<?>> {
    private String[] names;
    private int[] types;
    private final Iterator<List<?>> it;
    private final Map<String, Integer> nameToIndex;

    private int index = 0;
    private boolean firstNextWasCalled = false;
    private List<?> currentRecord = null;

    public ListRecordSet(String schema, String[] names, Iterable<List<?>> data) {
        this(schema, names, discoverTypes(names.length, data), data);
    }

    public ListRecordSet(String schema, String[] names, int[] types, Iterable<List<?>> data) {
        super(schema, names);
        this.names = names;
        this.types = types;
        this.it = data.iterator();
        nameToIndex = IntStream.range(0, names.length).boxed().collect(Collectors.toMap(i -> names[i], i -> i));
    }

    @Override
    protected List<?> getRecord() {
        if (currentRecord == null) {
            currentRecord = it.next();
        }
        return currentRecord;
    }


    @Override
    protected Map<String, Object> getData(List<?> record) {
        return IntStream.range(0, names.length).boxed().collect(Collectors.toMap(i -> names[i], record::get));
    }

    @Override
    protected Object getValue(List<?> record, String label) {
        return record.get(nameToIndex.get(label));
    }


    @Override
    public boolean next() throws SQLException {
        if (firstNextWasCalled && index == 1) {
            firstNextWasCalled = false;
            return true;
        }
        boolean result = it.hasNext();
        if (result) {
            index++;
            currentRecord = null;
        } else {
            done = true;
        }
        return result;
    }


    @Override
    protected List<?> getSampleRecord() {
        if (index > 0) {
            return getRecord();
        }

        try {
            if (next()) {
                firstNextWasCalled = true;
                return getRecord();
            }
        } catch (SQLException e) {
            return null;
        }

        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return new SimpleResultSetMetaData(null, schema, names, names, types);
    }

    private static int[] discoverTypes(int nColumns, Iterable<List<?>> data) {
        int[] types = new int[nColumns];
        int rowIndex = 0;
        for (List<?> row : data) {
            Integer[] rowTypes = row.stream().map(v -> v == null ? null : v.getClass()).map(c -> TypeConversion.sqlTypes.getOrDefault(c, 0)).toArray(Integer[]::new);
            for (int i = 0; i < nColumns; i++) {
                int type = types[i];
                int rowType = rowTypes[i];

                if (rowType == 0) {
                    continue;
                }

                if (type == 0) {
                    types[i] = rowType;
                    continue;
                }
                if (type != rowType) {
                    throw new IllegalArgumentException(String.format("Type of value [%d,%d] %s does not match already discovered type %s", rowIndex, i, rowType, type));
                }
            }
            rowIndex++;
        }
        System.out.println("types: " + Arrays.toString(types));
        return types;
    }

}
