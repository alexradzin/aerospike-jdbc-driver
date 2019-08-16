package com.nosqldriver.aerospike.sql;

import java.sql.SQLException;
import java.sql.Statement;

import static com.nosqldriver.aerospike.sql.TestDataUtils.testConn;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateUsingStatementExecuteTest extends UpdateTest {
    @Override
    protected void executeUpdate(String sql, int expectedRowCount) throws SQLException {
        Statement statement = testConn.createStatement();
        boolean result = statement.execute(sql);
        assertEquals(expectedRowCount != 0, result);
        assertEquals(expectedRowCount, statement.getUpdateCount());
    }
}
