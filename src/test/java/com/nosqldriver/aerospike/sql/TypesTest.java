package com.nosqldriver.aerospike.sql;

import com.nosqldriver.util.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Base64;
import java.util.Calendar;
import java.util.TimeZone;
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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

class TypesTest {
    private static final String TYPE_TEST_TABLE = "types_test";
    @BeforeEach
    @AfterEach
    void dropAll() {
        TestDataUtils.deleteAllRecords(NAMESPACE, TYPE_TEST_TABLE);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select text, integer, bigint, decimal, true_flag, false_flag from " + TYPE_TEST_TABLE,
            "select text, integer, bigint, decimal, true_flag, false_flag from " + TYPE_TEST_TABLE + " as p left join secondary_table as s on p.text=s.text",
            "select text, integer, bigint, decimal, true_flag, false_flag from " + TYPE_TEST_TABLE + " as p left join secondary_table as s on s.text=p.text"
    })
    void simpleTypes(String sql) throws Exception {
        long now = currentTimeMillis();
        testConn.createStatement().execute(format("insert into %s (PK, text, integer, bigint, decimal, true_flag, false_flag) values (1, 'hello', 123, %d, 3.1415926, 1, 0)", TYPE_TEST_TABLE, now));
        ResultSet rs = executeQuery(sql,
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
        assertValue(now, () -> rs.getDate(3).getTime(), () -> rs.getDate("bigint").getTime(), () -> rs.getTime(3).getTime(), () -> rs.getTime("bigint").getTime(), () -> rs.getTimestamp(3).getTime(), () -> rs.getTimestamp("bigint").getTime());
        Calendar calendar = Calendar.getInstance();
        assertValue(now, () -> rs.getDate(3, calendar).getTime(), () -> rs.getDate("bigint", calendar).getTime(), () -> rs.getTime(3, calendar).getTime(), () -> rs.getTime("bigint", calendar).getTime(), () -> rs.getTimestamp(3, calendar).getTime(), () -> rs.getTimestamp("bigint", calendar).getTime());
        Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        assertValue(now, () -> rs.getDate(3, utcCalendar).getTime(), () -> rs.getDate("bigint", utcCalendar).getTime(), () -> rs.getTime(3, utcCalendar).getTime(), () -> rs.getTime("bigint", utcCalendar).getTime(), () -> rs.getTimestamp(3, utcCalendar).getTime(), () -> rs.getTimestamp("bigint", utcCalendar).getTime());
        assertFalse(rs.next());
    }

    @Test
    void now() throws SQLException {
        long before = currentTimeMillis();
        ResultSet rs = testConn.createStatement().executeQuery("select now()");
        assertTrue(rs.next());
        long actual = rs.getLong(1);
        long after = currentTimeMillis();
        assertFalse(rs.next());
        assertTrue(actual >= before && actual <= after);
    }

    @Test
    void epochDefaultDateFormat() throws SQLException, ParseException {
        String date = "1969-07-21 02:56:00.000";
        assertEpoch(date, "yyyy-MM-dd HH:mm:ss", format("select epoch('%s')", date));
    }

    @Test
    void epochDateFormatWithTimeZone() throws SQLException, ParseException {
        String date = "1969-07-21 02:56:00 UTC";
        String format = "yyyy-MM-dd HH:mm:ss z";
        assertEpoch(date, format, format("select epoch('%s', '%s')", date, format));
    }

    @Test
    void epochWrongDateFormat() throws SQLException {
        String date = "foobar";
        ResultSet rs = testConn.createStatement().executeQuery(format("select epoch('%s')", date));
        assertTrue(rs.next());
        assertThrows(SQLException.class, () -> rs.getLong(1));
    }


    private void assertEpoch(String date, String format, String query) throws SQLException, ParseException {
        long expected = new SimpleDateFormat(format).parse(date).getTime();
        ResultSet rs = testConn.createStatement().executeQuery(query);
        assertTrue(rs.next());
        long actual = rs.getLong(1);
        assertFalse(rs.next());
        assertEquals(expected, actual);
    }

    @Test
    void date() throws SQLException, ParseException {
        DateFormat sqlDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); // used by SQL
        long before = currentTimeMillis();
        ResultSet rs = testConn.createStatement().executeQuery("select date(now())");
        assertTrue(rs.next());
        long actual = sqlDateFormat.parse(rs.getString(1)).getTime();
        long after = currentTimeMillis();
        assertFalse(rs.next());
        assertTrue(actual >= before && actual <= after);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year()",
            "select year(date())",
            "select year(date(now()))",
    })
    void year(String sql) throws SQLException {
        Calendar c = Calendar.getInstance();
        int year = c.get(Calendar.YEAR);
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(year, rs.getInt(1));
        assertFalse(rs.next());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year('2019-09-23') as year, month('2019-09-23') as month, dayofmonth('2019-09-23') as day",
            "select year('2019-09-23 14:49:03') as year, month('2019-09-23 14:49:03') as month, dayofmonth('2019-09-23 14:49:03') as day",
            "select year('2019-09-23 14:49:03.123') as year, month('2019-09-23 14:49:03.123') as month, dayofmonth('2019-09-23 14:49:03.123') as day",
            "select year('2019-09-23 14:49:03.123 IST') as year, month('2019-09-23 14:49:03.123 IST') as month, dayofmonth('2019-09-23 14:49:03.123 IST') as day",
            "select year('2019-09-23 14:49:03 IST') as year, month('2019-09-23 14:49:03 IST') as month, dayofmonth('2019-09-23 14:49:03 IST') as day",
    })
    void datePartsWithAliasesFromString(String sql) throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(2019, rs.getInt(1));
        assertEquals(2019, rs.getInt("year"));
        assertEquals(9, rs.getInt(2));
        assertEquals(9, rs.getInt("month"));
        assertEquals(23, rs.getInt(3));
        assertEquals(23, rs.getInt("day"));
        assertFalse(rs.next());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year('2019-09-23'), month('2019-09-23'), dayofmonth('2019-09-23')",
            "select year('2019-09-23 14:49:03'), month('2019-09-23 14:49:03'), dayofmonth('2019-09-23 14:49:03')",
            "select year('2019-09-23 14:49:03.123'), month('2019-09-23 14:49:03.123'), dayofmonth('2019-09-23 14:49:03.123')",
            "select year('2019-09-23 14:49:03.123 IST'), month('2019-09-23 14:49:03.123 IST'), dayofmonth('2019-09-23 14:49:03.123 IST')",
            "select year('2019-09-23 14:49:03 IST'), month('2019-09-23 14:49:03 IST'), dayofmonth('2019-09-23 14:49:03 IST')",
    })
    void datePartsFromString(String sql) throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(2019, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertEquals(23, rs.getInt(3));
        assertFalse(rs.next());
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year('2019-09-23 14:49:03.123'), month('2019-09-23 14:49:03.123'), dayofmonth('2019-09-23 14:49:03.123'), hour('2019-09-23 14:49:03.123'), minute('2019-09-23 14:49:03.123'), second('2019-09-23 14:49:03.123'), millisecond('2019-09-23 14:49:03.123')",
            "select YEAR('2019-09-23 14:49:03.123'), MONTH('2019-09-23 14:49:03.123'), DAYOFMONTH('2019-09-23 14:49:03.123'), HOUR('2019-09-23 14:49:03.123'), MINUTE('2019-09-23 14:49:03.123'), SECOND('2019-09-23 14:49:03.123'), MILLISECOND('2019-09-23 14:49:03.123')",
    })
    void timePartsFromString(String sql) throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(2019, rs.getInt(1));
        assertEquals(9, rs.getInt(2));
        assertEquals(23, rs.getInt(3));
        assertEquals(14, rs.getInt(4));
        assertEquals(49, rs.getInt(5));
        assertEquals(3, rs.getInt(6));
        assertEquals(123, rs.getInt(7));
        assertFalse(rs.next());
    }




    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select month('2019-09-23')",
    })
    void monthFromString(String sql) throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(9, rs.getInt(1));
        assertFalse(rs.next());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select 'JOHN LENNON' as name",
            "select concat('JOHN LENNON') as name",
            "select concat('JOHN', ' ', 'LENNON') as name",
            "select upper(concat('John', ' ', 'Lennon')) as name",
            "select concat(upper('John'), ' ', upper('Lennon')) as name",
            "select concat_ws(' ', 'JOHN', 'LENNON') as name",
    })
    void stringFunctionsWithAlias(String sql) throws SQLException {
        String expected = "JOHN LENNON";
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(expected, rs.getString(1));
        assertEquals(expected, rs.getString("name"));
        assertEquals(expected, rs.getNString(1));
        assertEquals(expected, rs.getNString("name"));
        assertEquals(expected, rs.getObject(1));
        assertEquals(expected, rs.getObject("name"));
        assertFalse(rs.next());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select 'JOHN LENNON'",
            "select concat('JOHN LENNON')",
            "select concat('JOHN', ' ', 'LENNON')",
            "select upper(concat('John', ' ', 'Lennon'))",
            "select concat(upper('John'), ' ', upper('Lennon'))",
            "select concat_ws(' ', 'JOHN', 'LENNON')",
            "select ucase(concat('John', ' ', 'Lennon'))",
    })
    void stringFunctionsWithoutAlias(String sql) throws SQLException {
        String expected = "JOHN LENNON";
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(expected, rs.getString(1));
        assertEquals(expected, rs.getNString(1));
        assertEquals(expected, rs.getObject(1));
        assertFalse(rs.next());
    }

    @Test
    void functionAscii() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery("select ascii('Hello')");
        assertTrue(rs.next());
        assertEquals('H', rs.getInt(1));
    }

    @Test
    void functionChar() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery("select char(65)");
        assertTrue(rs.next());
        assertEquals("A", rs.getString(1));
    }

    @Test
    void functionInstr() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery("select instr('Hello, world!', 'Hello'), instr('Hello, world!', 'world'), instr('Hello, world!', 'bye')");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(8, rs.getInt(2));
        assertEquals(0, rs.getInt(3));
    }

    @Test
    void functionLocate() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(
                "select locate('Hello', 'Hello, world!'), locate('world', 'Hello, world!'), locate('Hello, world!', 'bye'), locate('world', 'Hello, world!', 8), locate('world', 'Hello, world!', 3)"
        );
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        assertEquals(8, rs.getInt(2));
        assertEquals(0, rs.getInt(3));
        assertEquals(1, rs.getInt(4));
        assertEquals(6, rs.getInt(5));
    }

    @Test
    void functionSpace() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery("select space(0), space(1), space(5)");
        assertTrue(rs.next());
        assertEquals("", rs.getString(1));
        assertEquals(" ", rs.getString(2));
        assertEquals("     ", rs.getString(3));
    }

    @Test
    void functionTrim() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(
                "select trim('Hello'), trim(' Hello'), trim('Hello '), trim(' Hello '), ltrim(' Hello'), rtrim('Hello ')");
        assertTrue(rs.next());
        int n = rs.getMetaData().getColumnCount();
        for (int i = 0; i < n; i++) {
            assertEquals("Hello", rs.getString(i + 1));
        }
    }

    @Test
    void functionTrim2() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(
                "select ltrim('Hello'), ltrim(' Hello'), ltrim('Hello '), rtrim('Hello'), rtrim(' Hello'), rtrim('Hello ')");
        assertTrue(rs.next());
        // ltrim
        assertEquals("Hello", rs.getString(1));
        assertEquals("Hello", rs.getString(2));
        assertEquals("Hello ", rs.getString(3));
        // rtrim
        assertEquals("Hello", rs.getString(4));
        assertEquals(" Hello", rs.getString(5));
        assertEquals("Hello", rs.getString(6));
    }


    @Test
    void functionStrcmpEq() throws SQLException {
        functionStrcmp("select strcmp('', ''), strcmp('a', 'a'), strcmp('abc', 'abc')", 0);
    }

    @Test
    void functionStrcmpLt() throws SQLException {
        functionStrcmp("select strcmp('', 'a'), strcmp('a', 'b'), strcmp('ab', 'ac'), strcmp('abc', 'abcd')", -1);
    }

    @Test
    void functionStrcmpGt() throws SQLException {
        functionStrcmp("select strcmp('a', ''), strcmp('b', 'a'), strcmp('ac', 'ab'), strcmp('abcd', 'abc')", 1);
    }

    private void functionStrcmp(String sql, int expected) throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        int n = rs.getMetaData().getColumnCount();
        for (int i = 0; i < n; i++) {
            assertEquals(expected, rs.getInt(i + 1));
        }
    }

    @Test
    void functionReverse() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery("select reverse(''), reverse('Hello')");
        assertTrue(rs.next());
        assertEquals("", rs.getString(1));
        assertEquals("olleH", rs.getString(2));
    }

    @Test
    void functionBase64() throws SQLException {
        String str = "Hello";
        String base64 = Base64.getEncoder().encodeToString(str.getBytes());
        Base64.getDecoder().decode("");
        ResultSet rs = testConn.createStatement().executeQuery(format("select to_base64('%s'), from_base64('%s')", str, base64));
        assertTrue(rs.next());
        assertEquals(base64, rs.getString(1));
        assertEquals(str, new String(rs.getBytes(2)));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select 0, 1, 3.14, 'text', 1578140272981",
    })
    void types(String sql) throws SQLException, IOException {
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        assertTrue(rs.next());
        assertEquals(0, rs.getByte(1));
        assertEquals(0, rs.getShort(1));
        assertEquals(0, rs.getInt(1));
        assertFalse(rs.getBoolean(1));

        assertEquals(1, rs.getByte(2));
        assertEquals(1, rs.getShort(2));
        assertEquals(1, rs.getInt(2));
        assertTrue(rs.getBoolean(2));

        assertEquals(3.14f, rs.getFloat(3));
        assertEquals(3.14, rs.getDouble(3));
        assertEquals(3.14, rs.getBigDecimal(3).setScale(2, RoundingMode.FLOOR).doubleValue());

        assertEquals("text", rs.getString(4));
        assertEquals("text", new String(IOUtils.toByteArray(rs.getAsciiStream(4))));
        assertEquals("text", new String(IOUtils.toByteArray(rs.getBinaryStream(4))));
        assertEquals("text", IOUtils.toString(rs.getCharacterStream(4)));
        assertEquals("text", IOUtils.toString(rs.getNCharacterStream(4)));
        assertEquals("text", new String(IOUtils.toByteArray(rs.getUnicodeStream(4))));

        long timestamp = 1578140272981L;
        assertEquals(timestamp, rs.getDate(5).getTime());
        assertEquals(timestamp, rs.getTime(5).getTime());
        assertEquals(timestamp, rs.getTimestamp(5).getTime());

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