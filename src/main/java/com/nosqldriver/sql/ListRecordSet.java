package com.nosqldriver.sql;

import java.sql.ResultSetMetaData;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ListRecordSet extends ValueTypedResultSet<List<?>> {
    private final Iterator<List<?>> it;
    private final Map<String, Integer> nameToIndex;
    private List<?> currentRecord = null;

    public ListRecordSet(String schema, List<DataColumn> columns, Iterable<List<?>> data) {
        super(schema, columns);
        this.it = data.iterator();
        nameToIndex = IntStream.range(0, columns.size()).boxed().collect(Collectors.toMap(i -> columns.get(i).getName(), i -> i));
    }

    @Override
    protected List<?> getRecord() {
        return currentRecord;
    }


    @Override
    protected Map<String, Object> getData(List<?> record) {
        return IntStream.range(0, columns.size()).boxed().collect(Collectors.toMap(i -> columns.get(i).getName(), record::get));
    }

    @Override
    protected Object getValue(List<?> record, String label) {
        return record.get(nameToIndex.get(label));
    }


    protected boolean moveToNext() {
        boolean hasNext = it.hasNext();
        currentRecord = hasNext ? it.next() : null;
        return hasNext;
    }



    @Override
    public ResultSetMetaData getMetaData() {
        int[] types = new int[columns.size()];
        Integer[] types2 = columns.stream().map(DataColumn::getType).toArray(Integer[]::new);
        for (int i = 0; i < types.length; i++) {
            types[i] = types2[i];
        }
        return new SimpleResultSetMetaData(null, schema, columns.stream().map(DataColumn::getName).toArray(String[]::new), columns.stream().map(DataColumn::getLabel).toArray(String[]::new), types);
    }

    public static int[] discoverTypes(int nColumns, Iterable<List<?>> data) {
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
        return types;
    }

}
