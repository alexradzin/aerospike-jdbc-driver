package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeRootUrl;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TruncateTest {
    private final Connection testConn = getTestConnection();
    private final Connection conn = getConnection(aerospikeRootUrl);


    @BeforeEach
    void init() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        writeBeatles();
    }

    @AfterAll
    static void cleanup() {
        deleteAllRecords(NAMESPACE, PEOPLE);
    }

    @Test
    void truncateFullTable() throws SQLException {
        truncate(testConn, format("truncate table %s", PEOPLE));
    }

    @Test
    void truncateFullTableWithSchema() throws SQLException {
        truncate(conn, format("truncate table %s.%s", NAMESPACE, PEOPLE));
    }

    @Test
    void truncateFullTableWithSchemaConnectionWithSchema() throws SQLException {
        truncate(testConn, format("truncate table %s.%s", NAMESPACE, PEOPLE));
    }

    @Test
    void truncateFullTableWithPassedDate() throws SQLException {
        assertEquals(4, countEntries(PEOPLE));
        // This date is passed while the data is written now, so nothing will be removed
        conn.createStatement().execute(format("truncate table %s.%s '2020-06-20'", NAMESPACE, PEOPLE));
        assertEquals(4, countEntries(PEOPLE));
    }

    @Test
    void truncateNotExistingTable() {
        assertEquals("Table test.doesnotexist doesn't exist", assertThrows(SQLException.class, () -> testConn.createStatement().execute("truncate table doesnotexist")).getMessage());
    }

    private void truncate(Connection conn, String truncate) throws SQLException {
        assertEquals(4, countEntries(PEOPLE));
        conn.createStatement().execute(truncate);
        assertEquals(0, countEntries(PEOPLE));
    }

    private int countEntries(String table) throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(format("select count(*) from %s", table));
        int count = rs.next() ? rs.getInt(1) : 0;
        assertFalse(rs.next());
        return count;
    }

}
