package com.nosqldriver.aerospike.sql;

import java.sql.SQLException;

import static com.nosqldriver.aerospike.sql.TestDataUtils.testConn;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateUsingStatementUpdateTest extends UpdateTest {
    @Override
    protected void executeUpdate(String sql, int expectedRowCount) throws SQLException {
        int rowCount = testConn.createStatement().executeUpdate(sql);
        assertEquals(expectedRowCount, rowCount);
    }
}
