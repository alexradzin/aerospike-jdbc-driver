package com.nosqldriver.sql;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.nosqldriver.sql.SqlLiterals.sqlTypes;
import static com.nosqldriver.util.SneakyThrower.sneakyThrow;
import static java.lang.String.format;

public class ListRecordSet extends ValueTypedResultSet<List<?>> {
    private final Iterator<List<?>> it;
    private final Map<String, Integer> nameToIndex;
    private List<?> currentRecord = null;

    public ListRecordSet(Statement statement, String schema, String table, List<DataColumn> columns, Iterable<List<?>> data) {
        super(statement, schema, table, columns, columns1 -> discoverTypes(columns1, data));
        this.it = data.iterator();
        nameToIndex = IntStream.range(0, columns.size()).boxed().collect(Collectors.toMap(i -> columns.get(i).getName(), i -> i));
    }

    @Override
    protected List<?> getRecord() {
        return currentRecord;
    }

    @Override
    public boolean isLast() throws SQLException {
        return !isBeforeFirst() && it.hasNext();
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


    private static List<DataColumn> discoverTypes(List<DataColumn> columns, Iterable<List<?>> data) {
        if (columns.isEmpty() || (columns.size() == 1 && "*".equals(columns.get(0).getName()))) {
            return Collections.emptyList();
        }

        if(columns.stream().noneMatch(c -> c.getType() == 0)) {
            return columns;
        }

        int nColumns = columns.size();
        int rowIndex = 0;
        for (List<?> row : data) {
            Integer[] rowTypes = row.stream().map(v -> v == null ? null : v.getClass()).map(c -> sqlTypes.getOrDefault(c, 0)).toArray(Integer[]::new);
            for (int i = 0; i < nColumns; i++) {
                DataColumn column = columns.get(i);
                int type = column.getType();
                int rowType = rowTypes[i];

                if (rowType == 0) {
                    continue;
                }

                if (type == 0) {
                    type = rowType;
                    column.withType(type);
                    continue;
                }
                if (type != rowType) {
                    sneakyThrow(new SQLException(format("Type of value [%d,%d] %s does not match already discovered type %s", rowIndex, i, rowType, type)));
                }
            }
            rowIndex++;
        }
        return columns;
    }

    protected void setCurrentRecord(List<?> r) {
        currentRecord = r;
    }
}
