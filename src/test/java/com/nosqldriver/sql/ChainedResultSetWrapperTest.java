package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.TestDataUtils;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainedResultSetWrapperTest {
    private final String catalog = "catalog";
    private final String table = "table";

    @Test
    void noFieldsNoRecords() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(emptyList(), false);
        assertEquals(0, rs.getMetaData().getColumnCount());
        assertEmpty(rs);
    }

    @Test
    void noRecordsOneEmptySubResultSet() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(singletonList(createSsimpleResultSet(emptyList())), false);
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertEmpty(rs);
    }

    @Test
    void noRecordsSeveralEmptySubResultSet() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(asList(createSsimpleResultSet(emptyList()), createSsimpleResultSet(emptyList()), createSsimpleResultSet(emptyList())), false);
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertEmpty(rs);
    }


    @Test
    void oneRecord() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(singletonList(createSsimpleResultSet(singletonList(asList("John", "Smith", 1970)))), false);
        TestDataUtils.validate(rs.getMetaData(),
                DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER));
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void twoRecords() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(singletonList(createSsimpleResultSet(asList(
                asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942)))), false);
        TestDataUtils.validate(rs.getMetaData(),
                DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER));
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }


    @Test
    void twoRecordsThenEmptySet() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(asList(
                createSsimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942))),
                createSsimpleResultSet(emptyList())), false);
        TestDataUtils.validate(rs.getMetaData(),
                DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER));
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void emptyThenTwoRecords() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(asList(
                createSsimpleResultSet(emptyList()),
                createSsimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942)))), false);
        TestDataUtils.validate(rs.getMetaData(),
                DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER));
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }


    @Test
    void severalResultSets() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(asList(
                createSsimpleResultSet(emptyList()),
                createSsimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942))),
                createSsimpleResultSet(asList(asList("George", "Harrison", 1943), asList("Ringo", "Starr", 1940)))
        ), false);
        TestDataUtils.validate(rs.getMetaData(),
                DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER));
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("George", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Ringo", rs.getString("first_name"));
        assertFalse(rs.next());
    }


    private void assertEmpty(ResultSet rs) throws SQLException {
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.first());
        assertFalse(rs.next());
    }

    private ResultSet createSsimpleResultSet(List<List<?>> data) {
        return new ListRecordSet(catalog, table,
                asList(
                        DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                        DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER)),
                data);

    }
}