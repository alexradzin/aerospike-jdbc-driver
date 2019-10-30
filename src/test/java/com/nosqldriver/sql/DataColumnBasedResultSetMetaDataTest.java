package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.stream.IntStream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataColumnBasedResultSetMetaDataTest {
    private static final int MAX_BLOCK_SIZE = 128 * 1024;

    @Test
    void empty() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(emptyList());
        assertEquals(0, md.getColumnCount());
    }

    @Test
    void emptyIndexDoesNotExist() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(emptyList());
        assertEquals(0, md.getColumnCount());
        assertThrows(SQLException.class, () -> md.getCatalogName(1));
        assertThrows(SQLException.class, () -> md.getSchemaName(1));
        assertThrows(SQLException.class, () -> md.getTableName(1));
        assertThrows(SQLException.class, () -> md.getColumnName(1));
        assertThrows(SQLException.class, () -> md.getColumnLabel(1));
        assertThrows(SQLException.class, () -> md.getColumnType(1));
    }


    @Test
    void oneDataFieldWithoutType() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create("test", "people", "first_name", "given_name")
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "test", "people", "first_name", "given_name", 0, 0);
    }

    @Test
    void oneDataFieldWithoutTable() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create(null, null, "first_name", "given_name")
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "", "", "first_name", "given_name", 0, 0);
    }

    @Test
    void oneDataFieldWithCatalog() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create("catalog", "table", "first_name", "given_name")
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "catalog", "table", "first_name", "given_name", 0, 0);
    }

    @Test
    void oneDataFieldWithCatalog2() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create(null, null, "first_name", "given_name").withCatalog("catalog").withTable("table")
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "catalog", "table", "first_name", "given_name", 0, 0);
    }


    @Test
    void oneDataFieldWrongIndex() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create(null, null, "first_name", "given_name")
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "", "", "first_name", "given_name", 0, 0);
        IntStream.of(-1, 0, 2).boxed().forEach(index -> {
            assertThrows(SQLException.class, () -> md.getCatalogName(index));
            assertThrows(SQLException.class, () -> md.getTableName(index));
            assertThrows(SQLException.class, () -> md.getColumnName(index));
            assertThrows(SQLException.class, () -> md.getColumnLabel(index));
            assertThrows(SQLException.class, () -> md.getColumnType(index));
        });





    }

    @Test
    void oneDataFieldWithType() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create("foo", "persons", "last_name", "surname").withType(VARCHAR)
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "foo", "persons", "last_name", "surname", VARCHAR, MAX_BLOCK_SIZE);
    }

    @Test
    void severalDataFieldWithType() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(asList(
                DATA.create("test", "people", "first_name", "given_name").withType(VARCHAR),
                DATA.create("test", "people", "last_name", "surname").withType(VARCHAR),
                DATA.create("test", "people", "year_of_birth", "yob").withType(INTEGER),
                EXPRESSION.create("test", "people", "age", "age").withType(INTEGER)
        ));
        assertEquals(4, md.getColumnCount());
        assertColumn(md, 1, "test", "people", "first_name", "given_name", VARCHAR, MAX_BLOCK_SIZE);
        assertColumn(md, 2, "test", "people", "last_name", "surname", VARCHAR, MAX_BLOCK_SIZE);
        assertColumn(md, 3, "test", "people", "year_of_birth", "yob", INTEGER, 8);
        assertColumn(md, 4, "test", "people", "age", "age", INTEGER, 8);
    }


    @Test
    void dataAndHiddenFields() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(asList(
                HIDDEN.create("test", "people", "h1", null).withType(INTEGER),
                DATA.create("test", "people", "first_name", "given_name").withType(VARCHAR),
                HIDDEN.create("test", "people", "h2", null).withType(INTEGER),
                DATA.create("test", "people", "last_name", "surname").withType(VARCHAR),
                HIDDEN.create("test", "people", "h3", null).withType(INTEGER)
        ));
        assertEquals(2, md.getColumnCount());
        assertColumn(md, 1, "test", "people", "first_name", "given_name", VARCHAR, MAX_BLOCK_SIZE);
        assertColumn(md, 2, "test", "people", "last_name", "surname", VARCHAR, MAX_BLOCK_SIZE);
    }

    @Test
    void updateType() throws SQLException {
        DataColumnBasedResultSetMetaData md = new DataColumnBasedResultSetMetaData(asList(
                DATA.create("test", "people", "first_name", "given_name"),
                DATA.create("test", "people", "last_name", "surname")
        ));
        assertEquals(2, md.getColumnCount());
        assertColumn(md, 1, "test", "people", "first_name", "given_name", 0, 0);
        assertColumn(md, 2, "test", "people", "last_name", "surname", 0, 0);
        md.getColumns().get(0).withType(VARCHAR);
        assertColumn(md, 1, "test", "people", "first_name", "given_name", VARCHAR, MAX_BLOCK_SIZE);
        assertColumn(md, 2, "test", "people", "last_name", "surname", 0, 0);
    }



    private void assertColumn(ResultSetMetaData md, int column, String expectedCatalog, String expectedTable, String expectedName, String expectedLabel, int expectedType, int expectedPrecision) throws SQLException {
        assertEquals(expectedCatalog, md.getCatalogName(column));
        assertEquals(expectedTable, md.getTableName(column));
        assertEquals(expectedName, md.getColumnName(column));
        assertEquals(expectedLabel, md.getColumnLabel(column));
        assertEquals(expectedType, md.getColumnType(column));
        assertEquals(expectedPrecision, md.getPrecision(column));
        assertEquals(0, md.getScale(column));
    }

}