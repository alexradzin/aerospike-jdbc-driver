package com.nosqldriver.aerospike.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class UpdateUsingStatementExecuteQueryTest extends UpdateTest {
    @Override
    protected void executeUpdate(String sql, int expectedRowCount) throws SQLException {
        Statement statement = getTestConnection().createStatement();
        ResultSet rs = statement.executeQuery(sql);
        assertFalse(rs.next());
        assertEquals(expectedRowCount, statement.getUpdateCount());
    }
}
