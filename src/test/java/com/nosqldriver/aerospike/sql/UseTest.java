package com.nosqldriver.aerospike.sql;

import com.nosqldriver.util.ThrowingSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UseTest {
    private Connection rootConn;

    @BeforeEach
    void setUp() throws SQLException {
        rootConn = DriverManager.getConnection("jdbc:aerospike:localhost");
        TestDataUtils.deleteAllRecords(NAMESPACE, PEOPLE);
    }

    @AfterEach
    void tearDown() throws SQLException {
        rootConn.close();
        TestDataUtils.deleteAllRecords(NAMESPACE, PEOPLE);
    }

    @Test
    void executeUseStatement() throws SQLException {
        assertTrue(useStatementValidator(() -> rootConn.createStatement().execute("use test")));
    }

    @Test
    void executeUpdateUseStatement() throws SQLException {
        int n = useStatementValidator(() -> rootConn.createStatement().executeUpdate("use test"));
        assertEquals(1, n);
    }


    @Test
    void executeQueryUseStatement() throws SQLException {
        ResultSet rs =  useStatementValidator(() -> rootConn.createStatement().executeQuery("use test"));
        assertNotNull(rs);
        assertFalse(rs.next());
    }


    private <R> R useStatementValidator(ThrowingSupplier<R, SQLException> use) throws SQLException {
        assertThrows(SQLException.class, () -> rootConn.createStatement().executeUpdate("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)"));
        assertThrows(SQLException.class, () -> rootConn.createStatement().executeQuery("select count(*) from people"));
        assertThrows(SQLException.class, () -> rootConn.createStatement().executeQuery("select first_name from people"));

        //rootConn.createStatement().execute("use test");

        R result = use.get();
        assertEquals(1, rootConn.createStatement().executeUpdate("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)"));
        ResultSet rs = rootConn.createStatement().executeQuery("select count(*) from people");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        rs = rootConn.createStatement().executeQuery("select first_name from people");
        assertTrue(rs.next());
        assertEquals("John", rs.getString(1));

        return result;
    }


}
