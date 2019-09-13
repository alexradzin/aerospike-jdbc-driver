package com.nosqldriver.sql;

import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;

public class BasicArray extends SerialArray {
    private String schema;
    private final List<DataColumn> columns;

    public BasicArray(Array array, Map<String, Class<?>> map) throws SQLException {
        super(array, map);
        columns = columns(array.getBaseType());
    }

    public BasicArray(Array array) throws SQLException {
        super(array);
        columns = columns(array.getBaseType());
    }

    public BasicArray(String schema, String baseTypeName, Object[] elements) throws SQLException {
        super(new Array() {
            private final int baseType = ofNullable(SqlLiterals.sqlTypeByName.get(baseTypeName))
                    .orElseThrow(() -> new IllegalArgumentException(format("Unsupported array type %s", baseTypeName)));

            @Override
            public String getBaseTypeName() {
                return baseTypeName;
            }

            @Override
            public int getBaseType() {
                return baseType;
            }

            @Override
            public Object getArray() {
                return elements;
            }

            @Override
            public Object getArray(Map<String, Class<?>> map) {
                return elements;
            }

            @Override
            public Object getArray(long index, int count) {
                return elements;
            }

            @Override
            public Object getArray(long index, int count, Map<String, Class<?>> map) {
                return elements;
            }

            @Override
            public ResultSet getResultSet() {
                throw new IllegalStateException();
            }

            @Override
            public ResultSet getResultSet(Map<String, Class<?>> map) {
                throw new IllegalStateException();
            }

            @Override
            public ResultSet getResultSet(long index, int count) {
                throw new IllegalStateException();
            }

            @Override
            public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) {
                throw new IllegalStateException();
            }

            @Override
            public void free() {
                throw new IllegalStateException();
            }
        });
        this.schema = schema;
        //columns = asList(DATA.create(schema, null, "INDEX", "INDEX").withType(Types.INTEGER), DATA.create(schema, null, "VALUE", "VALUE").withType(getBaseType()));
        columns = columns(getBaseType());
    }

    private List<DataColumn> columns(int baseType) {
        return asList(DATA.create(schema, null, "INDEX", "INDEX").withType(Types.INTEGER), DATA.create(schema, null, "VALUE", "VALUE").withType(baseType));
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SerialException {
        return getResultSet(index, count, Collections.emptyMap());
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SerialException {
        return getResultSet(0, Integer.MAX_VALUE, map);
    }

    @Override
    public ResultSet getResultSet() throws SerialException {
        return getResultSet(0, Integer.MAX_VALUE, Collections.emptyMap());
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SerialException {
        AtomicInteger counter = new AtomicInteger((int)index);
        Iterable<List<?>> data = Arrays.stream(((Object[]) getArray()))
                .skip(index)
                .map(e -> asList(counter.incrementAndGet(), e))
                .collect(Collectors.toList());
        return new ListRecordSet(schema, null, columns, data);
    }
}
