package com.nosqldriver.sql;

import javax.sql.rowset.serial.SerialArray;
import javax.sql.rowset.serial.SerialException;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.lang.String.format;

public class BasicArray extends SerialArray {
    private String schema;
    private final List<DataColumn> columns;

    public BasicArray(Array array, Map<String, Class<?>> map) throws SQLException {
        super(array, map);
        columns = Arrays.asList(DATA.create(schema, null, "NUM", "NUM"), DATA.create(schema, null, "VALUE", "VALUE"));
    }

    public BasicArray(Array array) throws SQLException {
        super(array);
        columns = Arrays.asList(DATA.create(schema, null, "NUM", "NUM"), DATA.create(schema, null, "VALUE", "VALUE"));
    }

    public BasicArray(String schema, String baseTypeName, Object[] elements) throws SQLException {
        super(new Array() {
            private final int baseType = Optional.ofNullable(SqlLiterals.sqlTypeByName.get(baseTypeName))
                    .orElseThrow(() -> new IllegalArgumentException(format("Unsupported array type %s", baseTypeName)));

            @Override
            public String getBaseTypeName() throws SQLException {
                return baseTypeName;
            }

            @Override
            public int getBaseType() throws SQLException {
                return baseType;
            }

            @Override
            public Object getArray() throws SQLException {
                return elements;
            }

            @Override
            public Object getArray(Map<String, Class<?>> map) throws SQLException {
                return elements;
            }

            @Override
            public Object getArray(long index, int count) throws SQLException {
                return elements;
            }

            @Override
            public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
                return elements;
            }

            @Override
            public ResultSet getResultSet() throws SQLException {
                throw new IllegalStateException();
            }

            @Override
            public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
                throw new IllegalStateException();
            }

            @Override
            public ResultSet getResultSet(long index, int count) throws SQLException {
                throw new IllegalStateException();
            }

            @Override
            public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
                throw new IllegalStateException();
            }

            @Override
            public void free() throws SQLException {
                throw new IllegalStateException();
            }
        });
        this.schema = schema;
        columns = Arrays.asList(DATA.create(schema, null, "NUM", "NUM"), DATA.create(schema, null, "VALUE", "VALUE"));
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SerialException {
        return getResultSet(index, count, Collections.emptyMap());
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SerialException {
        return getResultSet(1, Integer.MAX_VALUE, map);
    }

    @Override
    public ResultSet getResultSet() throws SerialException {
        return getResultSet(1, Integer.MAX_VALUE, Collections.emptyMap());
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SerialException {
        AtomicInteger counter = new AtomicInteger(0);
        Iterable<List<?>> data = Arrays.stream(((Object[]) getArray())).map(e -> Arrays.asList(counter.incrementAndGet(), e)).collect(Collectors.toList());
        return new ListRecordSet(schema, null, columns, data);
    }
}
