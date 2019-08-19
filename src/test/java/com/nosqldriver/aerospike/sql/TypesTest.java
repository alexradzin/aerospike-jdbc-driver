package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.util.concurrent.Callable;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.testConn;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TypesTest {
    private static final String TYPE_TEST_TABLE = "types_test";
    @BeforeEach
    @AfterEach
    void dropAll() {
        TestDataUtils.deleteAllRecords(NAMESPACE, TYPE_TEST_TABLE);
    }


    @Test
    void simpleTypes() throws Exception {
        long now = currentTimeMillis();
        testConn.createStatement().execute(format("insert into %s (PK, text, integer, bigint, decimal, true_flag, false_flag) values (1, 'hello', 123, %d, 3.1415926, 1, 0)", TYPE_TEST_TABLE, now));
        ResultSet rs = executeQuery(format("select text, integer, bigint, decimal, true_flag, false_flag from %s", TYPE_TEST_TABLE),
                DATA.create(NAMESPACE, TYPE_TEST_TABLE, "text", "text").withType(VARCHAR),
                DATA.create(NAMESPACE, TYPE_TEST_TABLE, "integer", "integer").withType(BIGINT),
                DATA.create(NAMESPACE, TYPE_TEST_TABLE, "bigint", "bigint").withType(BIGINT),
                DATA.create(NAMESPACE, TYPE_TEST_TABLE, "decimal", "decimal").withType(DOUBLE),
                DATA.create(NAMESPACE, TYPE_TEST_TABLE, "true_flag", "true_flag").withType(BIGINT),
                DATA.create(NAMESPACE, TYPE_TEST_TABLE, "false_flag", "false_flag").withType(BIGINT)
        );

        assertTrue(rs.next());
        assertValue("hello", () -> rs.getString(1), () -> rs.getString("text"), () -> rs.getNString(1), () -> rs.getNString("text"));
        assertValue(123, () -> rs.getInt(2), () -> rs.getInt("integer"), () -> rs.getLong(2), () -> rs.getLong("integer"), () -> rs.getShort(2), () -> rs.getShort("integer"), () -> rs.getByte(2), () -> rs.getByte("integer"));
        assertValue(now, () -> rs.getLong(3), () -> rs.getLong("bigint"));
        assertValue(3.1415926, () -> rs.getDouble(4), () -> rs.getDouble("decimal"));
        assertValue(true, () -> rs.getBoolean(5), () -> rs.getBoolean("true_flag"));
        assertValue(false, () -> rs.getBoolean(6), () -> rs.getBoolean("false_flag"));
        assertFalse(rs.next());
    }


    @SafeVarargs
    private final <T> void assertValue(T expected, Callable<T>... getters) throws Exception {
        for (Callable<T> getter : getters) {
            if (expected instanceof Long || expected instanceof Integer || expected instanceof Short || expected instanceof Byte) {
                assertEquals(((Number) expected).longValue(), ((Number) getter.call()).longValue());
            } else if (expected instanceof Float || expected instanceof Double) {
                assertEquals(((Number) expected).doubleValue(), ((Number) getter.call()).doubleValue(), 0.001);
            } else {
                assertEquals(expected, getter.call());
            }
        }
    }
}