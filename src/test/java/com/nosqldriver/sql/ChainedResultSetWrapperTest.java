package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.TestDataUtils;
import com.nosqldriver.util.ThrowingFunction;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ChainedResultSetWrapperTest {
    private final String catalog = "catalog";
    private final String table = "table";

    @Test
    void noFieldsNoRecords() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, emptyList(), false);
        assertEquals(0, rs.getMetaData().getColumnCount());
        assertEmpty(rs);
    }

    @Test
    void noRecordsOneEmptySubResultSet() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, singletonList(createSimpleResultSet(emptyList())), false);
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertEmpty(rs);
    }

    @Test
    void noRecordsSeveralEmptySubResultSet() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(createSimpleResultSet(emptyList()), createSimpleResultSet(emptyList()), createSimpleResultSet(emptyList())), false);
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertEmpty(rs);
    }


    @Test
    void oneRecordNext() throws SQLException {
        oneRecord(ResultSet::next);
    }

    @Test
    void oneRecordFirst() throws SQLException {
        oneRecord(ResultSet::first);
    }

    @Test
    void oneRecordLast() throws SQLException {
        oneRecord(ResultSet::last);
    }

    void oneRecord(ThrowingFunction<ResultSet, Boolean, SQLException> move) throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, singletonList(createSimpleResultSet(singletonList(asList("John", "Smith", 1970)))), false);
        assertMetadata(rs);
        assertTrue(move.apply(rs));
        assertEquals("John", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void twoRecords() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, singletonList(createSimpleResultSet(asList(
                asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942)))), false);
        assertMetadata(rs);
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void twoRecordsLast() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, singletonList(createSimpleResultSet(asList(
                asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942)))), false);
        assertMetadata(rs);
        assertTrue(rs.last());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void twoRecordsThenEmptySet() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(
                createSimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942))),
                createSimpleResultSet(emptyList())), false);
        assertMetadata(rs);
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void emptyThenTwoRecords() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(
                createSimpleResultSet(emptyList()),
                createSimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942)))), false);
        assertMetadata(rs);
        assertTrue(rs.next());
        assertEquals("John", rs.getString("first_name"));
        assertTrue(rs.next());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void emptyThenTwoRecordsLast() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(
                createSimpleResultSet(emptyList()),
                createSimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942)))), false);
        assertMetadata(rs);
        assertTrue(rs.last());
        assertEquals("Paul", rs.getString("first_name"));
        assertFalse(rs.next());
    }


    @Test
    void severalResultSets() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(
                createSimpleResultSet(emptyList()),
                createSimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942))),
                createSimpleResultSet(asList(asList("George", "Harrison", 1943), asList("Ringo", "Starr", 1940)))
        ), false);
        assertMetadata(rs);
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

    @Test
    void severalResultSetsLast() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(
                createSimpleResultSet(emptyList()),
                createSimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942))),
                createSimpleResultSet(asList(asList("George", "Harrison", 1943), asList("Ringo", "Starr", 1940)))
        ), false);
        assertMetadata(rs);
        assertTrue(rs.last());
        assertEquals("Ringo", rs.getString("first_name"));
        assertFalse(rs.next());
    }

    @Test
    void severalResultSetsBeforeAndAfter() throws SQLException {
        ResultSet rs = new ChainedResultSetWrapper(null, asList(
                createSimpleResultSet(emptyList()),
                createSimpleResultSet(asList(asList("John", "Lennon", 1940), asList("Paul", "McCartney", 1942))),
                createSimpleResultSet(asList(asList("George", "Harrison", 1943), asList("Ringo", "Starr", 1940)))
        ), false);
        assertMetadata(rs);
        rs.beforeFirst();
        assertThrows(SQLException.class, () -> rs.getString("first_name"));

        rs.afterLast();
        assertThrows(SQLException.class, () -> rs.getString("first_name"));
    }



    private void assertEmpty(ResultSet rs) throws SQLException {
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.first());
        assertFalse(rs.next());
        assertFalse(rs.last());
    }

    private ResultSet createSimpleResultSet(List<List<?>> data) {
        return new ListRecordSet(null, catalog, table,
                asList(
                        DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                        DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER)),
                data);

    }

    private void assertMetadata(ResultSet rs) throws SQLException {
        TestDataUtils.validate(rs.getMetaData(),
                DATA.create(catalog, table, "first_name", "first_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "last_name", "last_name").withType(Types.VARCHAR),
                DATA.create(catalog, table, "year_of_birth", "year_of_birth").withType(Types.INTEGER));

    }
}