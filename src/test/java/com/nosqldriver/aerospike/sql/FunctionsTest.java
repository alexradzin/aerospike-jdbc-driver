package com.nosqldriver.aerospike.sql;

import com.nosqldriver.util.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeRootUrl;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FunctionsTest {
    @Test
    void strlen() throws SQLException {
        test("custom.function.strlen=" + Strlen.class.getName(), "select strlen('xyz')", rs -> assertEquals(3, rs.getInt(1)));
    }

    @Test
    void sqrt() throws SQLException {
        test("custom.function.sqrt=" + Sqrt.class.getName(), "select sqrt(25)", rs -> assertEquals(5, rs.getDouble(1)));
    }

    @Test
    void sqrtStrlen() throws SQLException {
        test(format("custom.function.strlen=%s&custom.function.sqrt=%s", Strlen.class.getName(), Sqrt.class.getName()),
                "select sqrt(strlen('abcd'))",
                rs -> assertEquals(2, rs.getDouble(1)));
    }

    @Test
    void sqrtAndStrlen() throws SQLException {
        test(format("custom.function.strlen=%s&custom.function.sqrt=%s", Strlen.class.getName(), Sqrt.class.getName()),
                "select sqrt(4), strlen('abc')",
                rs -> {assertEquals(2, rs.getDouble(1)); assertEquals(3, rs.getInt(2));});
    }


    private <T> void test(String params, String query, ThrowingConsumer<ResultSet, SQLException> f) throws SQLException {
        Connection conn = DriverManager.getConnection(format("%s?%s", aerospikeRootUrl, params));
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        f.accept(rs);
        assertFalse(rs.next());
    }

    public static class Strlen implements Function<String, Integer> {
        @Override
        public Integer apply(String s) {
            return s.length();
        }
    }

    public static class Sqrt implements Function<Double, Double> {
        @Override
        public Double apply(Double d) {
            return Math.sqrt(d);
        }
    }
}
