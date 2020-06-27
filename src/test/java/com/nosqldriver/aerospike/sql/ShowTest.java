package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeRootUrl;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getClient;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ShowTest {
    private static final String DATA = "data";
    private final Connection testConn = getTestConnection();
    private final Connection rootConn = getConnection(aerospikeRootUrl);

    @BeforeEach
    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, DATA);
        try {
            getClient().dropIndex(null, "test", DATA, "TEST_INDEX1");
        } catch (AerospikeException e) {
            assertTrue(e.getMessage().contains("Index does not exist on the system"));
        }
    }
    @Test
    void showSchemasTestConnection() throws SQLException {
        assertTrue(show(testConn, "show schemas").contains("test"));
    }

    @Test
    void showCatalogsTestConnection() throws SQLException {
        assertTrue(show(testConn, "show catalogs").contains("test"));
    }

    @Test
    void showTablesTestConnection() throws SQLException {
        testConn.createStatement().execute("insert into data (PK, text) values (1, 'one')");
        assertTrue(show(testConn, "show tables").contains("data"));
    }

    @Test
    void showSchemasRootConnection() throws SQLException {
        assertTrue(show(rootConn, "show schemas").contains("test"));
    }

    @Test
    void showCatalogsRootConnection() throws SQLException {
        assertTrue(show(rootConn, "show catalogs").contains("test"));
    }

    @Test
    void showTablesRootConnection() {
        assertEquals("No namespace selected", assertThrows(SQLException.class, () -> show(rootConn, "show tables")).getMessage());
    }

    @Test
    void showIndexes() throws SQLException {
        assertTrue(show(testConn, "show indexes").isEmpty());
        testConn.createStatement().execute(format("CREATE NUMERIC INDEX TEST_INDEX1 ON %s (text)", DATA));
        show(testConn, "show indexes");
        ResultSet rs = testConn.createStatement().executeQuery("show indexes");
        assertTrue(rs.next());
        assertEquals(DATA, rs.getString(1));
        assertEquals("TEST_INDEX1", rs.getString(2));
        assertEquals("text", rs.getString(3));
        assertEquals("NUMERIC", rs.getString(4));
        assertFalse(rs.next());
    }

    @Test
    void showSomethingWrong() {
        assertEquals(
                "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'something'",
                assertThrows(SQLException.class, () -> show(rootConn, "show something")).getMessage());
    }

    private Collection<String> show(Connection conn,  String statement) throws SQLException {
        return items(conn.createStatement().executeQuery(statement));
    }

    private Collection<String> items(ResultSet rs) throws SQLException {
        Collection<String> items = new ArrayList<>();
        while(rs.next()) {
            items.add(rs.getString(1));
        }
        return items;
    }
}
