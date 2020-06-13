package com.nosqldriver.aerospike.sql;

import java.sql.SQLException;
import java.sql.Statement;

import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class UpdateUsingStatementExecuteTest extends UpdateTest {
    @Override
    protected void executeUpdate(String sql, int expectedRowCount) throws SQLException {
        Statement statement = getTestConnection().createStatement();
        boolean result = statement.execute(sql);
        assertFalse(result);
        assertEquals(expectedRowCount, statement.getUpdateCount());
    }
}
