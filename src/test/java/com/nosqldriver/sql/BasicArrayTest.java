package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static com.nosqldriver.sql.SqlLiterals.sqlTypeByName;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BasicArrayTest {
    private final String[] names = new String[] {"John", "Paul", "George", "Ringo"};

    @Test
    void varcharArray() throws SQLException {
        assertVarcharArray(new BasicArray("schema", "varchar", names));
    }

    @Test
    void varcharArrayThatWrapsOtherArray() throws SQLException {
        assertVarcharArray(new BasicArray(new BasicArray("schema", "varchar", names)));
    }

    @Test
    void varcharArrayThatWrapsOtherArrayWithEmptyMap() throws SQLException {
        assertVarcharArray(new BasicArray(new BasicArray("schema", "varchar", names), emptyMap()));
    }

    @Test
    void varcharArrayThatWrapsOtherArrayWithEmptyMapAndRetrievesResultSetWithEmptyMap() throws SQLException {
        assertVarcharArrayResultSetWithEmptyMap(new BasicArray(new BasicArray("schema", "varchar", names), emptyMap()));
    }


    @Test
    void emptyVarcharArray() throws SQLException {
        Array a = new BasicArray(new BasicArray("schema", "varchar", new String[]{}));
        assertEquals(Types.VARCHAR, a.getBaseType());
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(1, 1));
        assertFalse(assertResultSet(a.getResultSet(), a.getBaseType()).next());
    }

    @Test
    void resultSetOfSlice() throws SQLException {
        Array a = new BasicArray("schema", "varchar", names);
        ResultSet rs = a.getResultSet(1, 3);
        int count = 0;
        for (int i = 1; rs.next(); i++, count++) {
            assertEquals(i + 1, rs.getInt(1));
            assertEquals(names[i], rs.getString(2));
        }
        assertEquals(3, count);
    }

    @Test
    void createArraysOfValidTypes() throws SQLException {
        for(String type : sqlTypeByName.keySet()) {
            assertEquals(0, ((Object[])new BasicArray("schema", type, new Object[0]).getArray()).length);
        }
    }

    @Test
    void createArraysOfInvalidType() {
        assertThrows(IllegalArgumentException.class, () -> assertEquals(0, ((Object[])new BasicArray("schema", "unknown", new Object[0]).getArray()).length));
    }

    private void assertVarcharArray(Array a) throws SQLException {
        assertVarcharArray(a, a.getResultSet());
    }

    private void assertVarcharArrayResultSetWithEmptyMap(Array a) throws SQLException {
        assertVarcharArray(a, a.getResultSet(emptyMap()));
    }


    private void assertVarcharArray(Array a, ResultSet rs) throws SQLException {
        assertArrayEquals(names, (Object[])a.getArray());
        assertEquals(Types.VARCHAR, a.getBaseType());
        assertArrayEquals(new String[] {"Paul", "George", "Ringo"}, (Object[])a.getArray(1, 3));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(1, 4)); // right index out of bounds
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(4, 1)); // left index out of bounds
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(-1, 1)); // left index out of bounds

        //ResultSet rs = assertResultSet(a.getResultSet(), a.getBaseType());
        assertResultSet(a.getResultSet(), a.getBaseType());
        int i = 0;
        for (; rs.next(); i++) {
            assertEquals(i + 1, rs.getInt(1));
            assertEquals(names[i], rs.getString(2));
        }
        assertEquals(names.length, i);
    }



    private ResultSet assertResultSet(ResultSet rs, int baseType) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("INDEX", md.getColumnName(1));
        assertEquals("INDEX", md.getColumnLabel(1));
        assertEquals(Types.INTEGER, md.getColumnType(1));
        assertEquals("VALUE", md.getColumnName(2));
        assertEquals("VALUE", md.getColumnLabel(2));
        assertEquals(baseType, md.getColumnType(2));
        return rs;
    }
}