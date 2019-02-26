package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class SelectTest {
    @Test
    void select() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
        assertNotNull(conn);

        ResultSet rs = conn.createStatement().executeQuery("select * from people");
        while (rs.next()) {
            System.out.println(rs.getString("first_name") + " | " + rs.getString("last_name"));
        }

    }
}
