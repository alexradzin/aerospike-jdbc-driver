package com.nosqldriver.aerospike.sql;

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UseTest {
    @BeforeEach
    @AfterEach
    void dropAll() {
        TestDataUtils.deleteAllRecords(NAMESPACE, PEOPLE);
    }


    @Test
    void useStatementValidator() throws SQLException {
        Connection rootConn = DriverManager.getConnection("jdbc:aerospike:localhost");

        assertThrows(SQLException.class, () -> rootConn.createStatement().executeUpdate("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)"));
        assertThrows(SQLException.class, () -> rootConn.createStatement().executeQuery("select count(*) from people"));
        assertThrows(SQLException.class, () -> rootConn.createStatement().executeQuery("select first_name from people"));

        rootConn.createStatement().execute("use test");
        assertEquals(1, rootConn.createStatement().executeUpdate("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)"));
        ResultSet rs = rootConn.createStatement().executeQuery("select count(*) from people");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        rs = rootConn.createStatement().executeQuery("select first_name from people");
        assertTrue(rs.next());
        assertEquals("John", rs.getString(1));
    }


}
