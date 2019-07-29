package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListRecordSetTest {
    private final String catalog = "catalog";
    private final String table = "table";

    @Test
    void noFieldsNoRecords() throws SQLException {
        ResultSet rs = new ListRecordSet("schema", "table", emptyList(), emptyList());
        assertEquals(0, rs.getMetaData().getColumnCount());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.first());
        assertFalse(rs.next());
    }

    @Test
    void columnsDefinedNoRecords() throws SQLException {
        ResultSet rs = new ListRecordSet(
                catalog,
                table,
                asList(
                        DATA.create(catalog, table, "first_name", "given_name"),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth"),
                        EXPRESSION.create(catalog, table, "year()-year_of_birth", "age")),
                emptyList());

        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(3, md.getColumnCount());
        assertEquals("first_name", md.getColumnName(1));
        assertEquals("given_name", md.getColumnLabel(1));
        assertEquals("year_of_birth", md.getColumnName(2));
        assertEquals("year_of_birth", md.getColumnLabel(2));
        assertEquals("year()-year_of_birth", md.getColumnName(3));
        assertEquals("age", md.getColumnLabel(3));

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.first());
        assertFalse(rs.next());
    }


    @Test
    void noFieldsOneRecord() throws SQLException {
        ResultSet rs = new ListRecordSet("schema", "table",
                asList(
                        DATA.create(catalog, table, "first_name", "first_name"),
                        DATA.create(catalog, table, "last_name", "last_name"),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth")),
                singletonList(asList("John", "Smith", 1970)));
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.first());
        assertEquals("John", rs.getString(1));
        assertEquals("Smith", rs.getString(2));
        assertEquals(1970, rs.getInt(3));
        assertFalse(rs.next());
    }

}