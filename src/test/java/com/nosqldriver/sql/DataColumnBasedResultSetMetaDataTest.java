package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

class DataColumnBasedResultSetMetaDataTest {
    @Test
    void empty() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(emptyList());
        assertEquals(0, md.getColumnCount());
    }

    @Test
    void oneDataFieldWithoutType() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create("test", "people", "first_name", "given_name")
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "test", "people", "first_name", "given_name", 0);
    }

    @Test
    void oneDataFieldWithType() throws SQLException {
        ResultSetMetaData md = new DataColumnBasedResultSetMetaData(singletonList(
                DATA.create("foo", "persons", "last_name", "surname").withType(VARCHAR)
        ));
        assertEquals(1, md.getColumnCount());
        assertColumn(md, 1, "foo", "persons", "last_name", "surname", VARCHAR);
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
        assertColumn(md, 1, "test", "people", "first_name", "given_name", VARCHAR);
        assertColumn(md, 2, "test", "people", "last_name", "surname", VARCHAR);
        assertColumn(md, 3, "test", "people", "year_of_birth", "yob", INTEGER);
        assertColumn(md, 4, "test", "people", "age", "age", INTEGER);
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
        assertColumn(md, 1, "test", "people", "first_name", "given_name", VARCHAR);
        assertColumn(md, 2, "test", "people", "last_name", "surname", VARCHAR);
    }

    @Test
    void updateType() throws SQLException {
        DataColumnBasedResultSetMetaData md = new DataColumnBasedResultSetMetaData(asList(
                DATA.create("test", "people", "first_name", "given_name"),
                DATA.create("test", "people", "last_name", "surname")
        ));
        assertEquals(2, md.getColumnCount());
        assertColumn(md, 1, "test", "people", "first_name", "given_name", 0);
        assertColumn(md, 2, "test", "people", "last_name", "surname", 0);
        md.getColumns().get(0).withType(VARCHAR);
        assertColumn(md, 1, "test", "people", "first_name", "given_name", VARCHAR);
        assertColumn(md, 2, "test", "people", "last_name", "surname", 0);
    }



    private void assertColumn(ResultSetMetaData md, int column, String expectedCatalog, String expectedTable, String expectedName, String expectedLabel, int expectedType) throws SQLException {
        assertEquals(expectedCatalog, md.getCatalogName(column));
        assertEquals(expectedTable, md.getTableName(column));
        assertEquals(expectedName, md.getColumnName(column));
        assertEquals(expectedLabel, md.getColumnLabel(column));
        assertEquals(expectedType, md.getColumnType(column));
    }

}