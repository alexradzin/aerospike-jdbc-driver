package com.nosqldriver.sql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import static com.nosqldriver.sql.SqlLiterals.sqlTypeByName;
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
    void emptyVarcharArray() throws SQLException {
        Array a = new BasicArray(new BasicArray("schema", "varchar", new String[]{}));
        assertEquals(Types.VARCHAR, a.getBaseType());
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(1, 1));
        assertFalse(getAndCheckResultSet(a).next());
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
        Assertions.assertThrows(IllegalArgumentException.class, () -> assertEquals(0, ((Object[])new BasicArray("schema", "unknown", new Object[0]).getArray()).length));
    }


    private void assertVarcharArray(Array a) throws SQLException {
        assertArrayEquals(names, (Object[])a.getArray());
        assertEquals(Types.VARCHAR, a.getBaseType());
        assertArrayEquals(new String[] {"Paul", "George", "Ringo"}, (Object[])a.getArray(1, 3));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(1, 4)); // right index out of bounds
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(4, 1)); // left index out of bounds
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> a.getArray(-1, 1)); // left index out of bounds

        ResultSet rs = getAndCheckResultSet(a);
        int i = 0;
        for (; rs.next(); i++) {
            assertEquals(i + 1, rs.getInt(1));
            assertEquals(names[i], rs.getString(2));
        }
        assertEquals(names.length, i);
    }



    private ResultSet getAndCheckResultSet(Array array) throws SQLException {
        ResultSet rs = array.getResultSet();
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(2, md.getColumnCount());
        assertEquals("INDEX", md.getColumnName(1));
        assertEquals("INDEX", md.getColumnLabel(1));
        assertEquals(Types.INTEGER, md.getColumnType(1));
        assertEquals("VALUE", md.getColumnName(2));
        assertEquals("VALUE", md.getColumnLabel(2));
        assertEquals(array.getBaseType(), md.getColumnType(2));
        return rs;
    }
}