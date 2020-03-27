package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.sql.ByteArrayBlob;
import com.nosqldriver.sql.StringClob;
import com.nosqldriver.util.IOUtils;
import com.nosqldriver.util.VariableSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getClient;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests of INSERT SQL statement
 */
class InsertTest {
    private static final String DATA = "data";
    private IAerospikeClient client = getClient();
    private Connection testConn = getTestConnection();

    @BeforeEach
    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, DATA);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)",
            "insert into test.people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)",
    })
    void insertOneRow(String sql) throws SQLException {
        insert(sql, 1);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("year_of_birth", 1940L);
        expectedData.put("kids_count", 2L);
        assertEquals(expectedData, record.bins);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "insert into data (PK, number, real, text, nothing) values (1, 1, 3.14, 'something', NULL)",
            "insert into data (PK, number, real, text, nothing) values (1, 1, 3.14, 'something', null)",
    })
    void insertNull(String sql) throws SQLException {
        insert(sql, 1);
        Record record = client.get(null, new Key("test", "data", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("number", 1L);
        expectedData.put("real", 3.14);
        expectedData.put("text", "something");
        assertEquals(expectedData, record.bins);
    }

    @Test
    void insertDuplicateRow() throws SQLException {
        insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("year_of_birth", 1940L);
        expectedData.put("kids_count", 2L);
        assertEquals(expectedData, record.bins);

        SQLException e = assertThrows(SQLException.class, () ->
            insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1)
        );
        assertTrue(e.getMessage().contains("Duplicate entries"));
    }

    @Test
    void insertIgnoreDuplicateRow() throws SQLException {
        insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        assertEquals("John", record.getString("first_name"));

        insert("insert ignore into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        assertNotNull(record);
        assertEquals("John", record.getString("first_name"));
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2), (2, 2, 'Paul', 'McCartney', 1942, 5)",
            "" +
                    "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count, dummy) values (1, 1, 'John', 'Lennon', 1940, 2, 'a;aa');" +
                    "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count, dummy) values(2, 2, 'Paul', 'McCartney', 1942, 5, 'bb;b')",
            "" +
                    "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count, dummy) values (1, 1, 'John', 'Lennon', 1940, 2, 'a;aa');" +
                    "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count, dummy) values(2, 2, 'Paul', 'McCartney', 1942, 5, 'bb;b');",
            "" +
                    "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count, dummy) values (1, 1, 'John', 'Lennon', 1940, 2, 'a;aa');" +
                    "insert into people (PK, id, first_name, last_name, year_of_birth, kids_count, dummy) values(2, 2, 'Paul', 'McCartney', 1942, 5, 'bb;b');\n\n    \n",
    })
    void insertSeveralRows(String sql) throws SQLException {
        insert(sql, 2);
        assertEquals("John", client.get(null, new Key("test", "people", 1)).getString("first_name"));
        assertEquals("Paul", client.get(null, new Key("test", "people", 2)).getString("first_name"));
    }

    @Test
    void insertSeveralRowsUsingExecute() throws SQLException {
        assertTrue(testConn.createStatement().execute("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2), (2, 2, 'Paul', 'McCartney', 1942, 5)"));
        assertEquals("John", client.get(null, new Key("test", "people", 1)).getString("first_name"));
        assertEquals("Paul", client.get(null, new Key("test", "people", 2)).getString("first_name"));
    }



    @Test
    void insertOneRowUsingPreparedStatement() throws SQLException {
        PreparedStatement ps = testConn.prepareStatement("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (?, ?, ?, ?, ?, ?)");
        ps.setInt(1, 1);
        ps.setInt(2, 1);
        ps.setString(3, "John");
        ps.setString(4, "Lennon");
        ps.setInt(5, 1940);
        ps.setInt(6, 2);
        int rowCount = ps.executeUpdate();
        assertEquals(1, rowCount);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("year_of_birth", 1940L);
        expectedData.put("kids_count", 2L);
        assertEquals(expectedData, record.bins);
    }

    @Test
    void insertAndSelectOneRowUsingPreparedStatementVariousTypes() throws SQLException, IOException {
        long now = currentTimeMillis();
        insertOneRowUsingPreparedStatementVariousTypes(now);
        assertOneSelectedRowUsingPreparedStatementVariousTypes(now, "select byte, short, int, long, boolean, float_number, double_number, bigdecimal, string, nstring, blob, clob, nclob, t, ts, d, url, nothing from data where PK=?", 1);
    }

    @Test
    void insertAndSelectWithJoinOneRowUsingPreparedStatementVariousTypes() throws SQLException, IOException {
        long now = currentTimeMillis();
        insertOneRowUsingPreparedStatementVariousTypes(now);
        assertOneSelectedRowUsingPreparedStatementVariousTypes(now, "select l.byte, l.short, l.int, l.long, l.boolean, l.float_number, l.double_number, l.bigdecimal, l.string, l.nstring, l.blob, l.clob, l.nclob, l.t, l.ts, l.d, l.url, l.nothing from data as l left join data2 as r on l.byte=r.id where PK=?", 1);
    }


    @VisibleForPackage // visible for tests
    @SuppressWarnings("unused") // referenced from annotation VariableSource
    private static Stream<Arguments> selectOneRowUsingPreparedStatementVariousTypes = Stream.of(
            Arguments.of("select byte, short, int, long, boolean, float_number, double_number, bigdecimal, string, nstring, blob, clob, nclob, t, ts, d, url, nothing from data where PK=?", 1),
            Arguments.of("select byte, short, int, long, boolean, float_number, double_number, bigdecimal, string, nstring, blob, clob, nclob, t, ts, d, url, nothing from data where PK!=?", 12345),
            Arguments.of("select l.byte, l.short, l.int, l.long, l.boolean, l.float_number, l.double_number, l.bigdecimal, l.string, l.nstring, l.blob, l.clob, l.nclob, l.t, l.ts, l.d, l.url, l.nothing from data as l left join data2 as r on l.byte=r.id where PK=?", 1)
    );
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @VariableSource("selectOneRowUsingPreparedStatementVariousTypes")
    void insertAndSelectOneRowUsingPreparedStatementVariousTypes(String sql, int value) throws SQLException, IOException {
        long now = currentTimeMillis();
        insertOneRowUsingPreparedStatementVariousTypes(now);
        assertOneSelectedRowUsingPreparedStatementVariousTypes(now, sql, value);
    }


    private void insertOneRowUsingPreparedStatementVariousTypes(long now) throws SQLException, IOException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement(
                "insert into data (PK, byte, short, int, long, boolean, float_number, double_number, bigdecimal, string, nstring, blob, clob, nclob, t, ts, d, url, nothing) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        String helloWorld = "hello, world!";
        String google = "http://www.google.com";
        ps.setInt(1, 1);
        ps.setByte(2, (byte)8);
        ps.setShort(3, (short)512);
        ps.setInt(4, 1024);
        ps.setLong(5, now);
        ps.setBoolean(6, true);
        ps.setFloat(7, 3.14f);
        ps.setDouble(8, 2.718281828);
        ps.setBigDecimal(9, BigDecimal.valueOf(Math.PI));

        ps.setString(10, "hello");
        ps.setNString(11, helloWorld);

        Blob blob = testConn.createBlob();
        blob.setBytes(1, helloWorld.getBytes());
        ps.setBlob(12, blob);

        Clob clob = testConn.createClob();
        clob.setString(1, helloWorld);
        ps.setClob(13, clob);

        NClob nclob = testConn.createNClob();
        nclob.setString(1, helloWorld);
        ps.setNClob(14, nclob);

        ps.setTime(15, new Time(now));
        ps.setTimestamp(16, new Timestamp(now));
        ps.setDate(17, new java.sql.Date(now));
        ps.setURL(18, new URL(google));
        ps.setNull(19, Types.VARCHAR);


        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertEquals(8, record1.getByte("byte"));
        assertEquals((short)512, record1.getValue("short")); // work around on Aerospike client bug: getShort() fails on casting
        assertEquals(1024, record1.getInt("int"));
        assertEquals(now, record1.getLong("long"));
        assertTrue(record1.getBoolean("boolean"));
        assertEquals(3.14f, record1.getFloat("float_number"));
        assertEquals(2.718281828, record1.getDouble("double_number"));
        assertEquals(Math.PI, record1.getDouble("bigdecimal"));
        assertEquals("hello", record1.getString("string"));
        assertEquals(helloWorld, record1.getString("nstring"));
        assertArrayEquals(helloWorld.getBytes(), (byte[])record1.getValue("blob"));
        assertEquals(helloWorld, record1.getString("clob"));
        assertEquals(helloWorld, record1.getString("nclob"));
        assertEquals(new Time(now), record1.getValue("t"));
        assertEquals(new Timestamp(now), record1.getValue("ts"));
        assertEquals(new java.sql.Date(now), record1.getValue("d"));
        assertEquals(google, record1.getString("url"));
        assertNull(record1.getString("nothing"));
    }


    private void assertOneSelectedRowUsingPreparedStatementVariousTypes(long now, String query, int pk) throws SQLException, IOException {
        String helloWorld = "hello, world!";
        String google = "http://www.google.com";
        PreparedStatement ps = testConn.prepareStatement(query);
        ps.setInt(1, pk);

        ResultSet rs = ps.executeQuery();
        ResultSetMetaData md = rs.getMetaData();

        int n = md.getColumnCount();

        for (int i = 1; i <= n; i++) {
            assertEquals(DATA, md.getTableName(i));
            assertEquals(NAMESPACE, md.getCatalogName(i));
        }

        // All ints except short are  stored as longs
        assertEquals(Types.BIGINT, md.getColumnType(1));
        assertEquals(Types.SMALLINT, md.getColumnType(2));
        assertEquals(Types.BIGINT, md.getColumnType(3));
        assertEquals(Types.BIGINT, md.getColumnType(4));
        assertEquals(Types.BIGINT, md.getColumnType(5));

        assertEquals(Types.DOUBLE, md.getColumnType(6));
        assertEquals(Types.DOUBLE, md.getColumnType(7));
        assertEquals(Types.DOUBLE, md.getColumnType(8));

        assertEquals(Types.VARCHAR, md.getColumnType(9));
        assertEquals(Types.VARCHAR, md.getColumnType(10));
        assertEquals(Types.BLOB, md.getColumnType(11));
        // There is not way to distinguish between clob, nclob and string. All are VARCHARs
        assertEquals(Types.VARCHAR, md.getColumnType(12));
        assertEquals(Types.VARCHAR, md.getColumnType(13));
        assertEquals(Types.TIME, md.getColumnType(14));
        assertEquals(Types.TIMESTAMP, md.getColumnType(15));
        assertEquals(Types.DATE, md.getColumnType(16));
        assertEquals(Types.VARCHAR, md.getColumnType(17));
        assertEquals(Types.NULL, md.getColumnType(18));

        assertTrue(rs.first());

        assertEquals(8, rs.getByte(1));
        assertEquals(8, rs.getByte("byte"));

        assertEquals((short)512, rs.getShort(2));
        assertEquals((short)512, rs.getShort("short"));

        assertEquals(1024, rs.getInt(3));
        assertEquals(1024, rs.getInt("int"));

        assertEquals(now, rs.getLong(4));
        assertEquals(now, rs.getLong("long"));

        assertTrue(rs.getBoolean(5));
        assertTrue(rs.getBoolean("boolean"));

        assertEquals(3.14f, rs.getFloat(6));
        assertEquals(3.14f, rs.getFloat("float_number"));

        assertEquals(2.718281828, rs.getDouble(7));
        assertEquals(2.718281828, rs.getDouble("double_number"));

        assertEquals(Math.PI, rs.getBigDecimal(8).doubleValue());
        assertEquals(Math.PI, rs.getBigDecimal("bigdecimal").doubleValue());
        assertEquals(3.14, rs.getBigDecimal(8, 2).doubleValue());
        assertEquals(3.14, rs.getBigDecimal("bigdecimal", 2).doubleValue());

        assertEquals("hello", rs.getString(9));
        assertEquals("hello", rs.getString("string"));
        assertEquals("hello", IOUtils.toString(rs.getClob("string").getCharacterStream()));
        assertEquals("hello", IOUtils.toString(rs.getNClob("string").getCharacterStream()));

        assertEquals(helloWorld, rs.getNString(10));
        assertEquals(helloWorld, rs.getNString("nstring"));

        assertArrayEquals(helloWorld.getBytes(), getBytes(rs.getBlob(11)));
        assertArrayEquals(helloWorld.getBytes(), getBytes(rs.getBlob("blob")));
        assertArrayEquals(helloWorld.getBytes(), rs.getBytes(11));
        assertArrayEquals(helloWorld.getBytes(), rs.getBytes("blob"));

        assertEquals(helloWorld, getString(rs.getClob(12)));
        assertEquals(helloWorld, getString(rs.getClob("clob")));

        assertEquals(helloWorld, getString(rs.getClob(13)));
        assertEquals(helloWorld, getString(rs.getClob("nclob")));

        assertEquals(new Time(now), rs.getTime(14));
        assertEquals(new Time(now), rs.getTime("t"));

        assertEquals(new Timestamp(now), rs.getTimestamp(15));
        assertEquals(new Timestamp(now), rs.getTimestamp("ts"));

        assertEquals(new java.sql.Date(now), rs.getDate(16));
        assertEquals(new java.sql.Date(now), rs.getDate("d"));

        assertEquals(google, rs.getString(17));
        assertEquals(google, rs.getString("url"));

        assertFalse(rs.wasNull());
        assertNull(rs.getString(18));
        assertNull(rs.getString("nothing"));
        assertTrue(rs.wasNull());

        assertFalse(rs.next());
    }


    @Test
    void insertBytes() throws SQLException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, bytes) values (?, ?)");

        String text = "hello, blob!";
        byte[] bytes = text.getBytes();

        ps.setInt(1, 1);
        ps.setBytes(2, bytes);

        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertArrayEquals(bytes, (byte[])record1.getValue("bytes"));

        try(ResultSet rs = testConn.createStatement().executeQuery("select bytes from data")) {
            assertTrue(rs.next());
            assertArrayEquals(bytes, rs.getBytes("bytes"));
            assertFalse(rs.next());
        }
    }



    @Test
    void insertBlobs() throws SQLException, IOException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, blob, input_stream, limited_is) values (?, ?, ?, ?)");

        String text = "hello, blob!";
        byte[] bytes = text.getBytes();
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, text.getBytes());


        ps.setInt(1, 1);
        ps.setBlob(2, blob);
        ps.setBlob(3, new ByteArrayInputStream(bytes));
        ps.setBlob(4, new ByteArrayInputStream(bytes), bytes.length);

        InputStream in1 = Mockito.mock(InputStream.class);
        when(in1.read(any(byte[].class), any(int.class), any(int.class))).thenThrow(EOFException.class);
        assertThrows(SQLException.class, () -> ps.setBlob(4, in1, bytes.length));

        InputStream in2 = Mockito.mock(InputStream.class);
        when(in2.read(any(byte[].class), any(int.class), any(int.class))).thenThrow(IOException.class);
        assertThrows(SQLException.class, () -> ps.setBlob(4, in2, bytes.length));

        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertArrayEquals(bytes, (byte[])record1.getValue("blob"));
        assertArrayEquals(bytes, (byte[])record1.getValue("input_stream"));
        assertArrayEquals(bytes, (byte[])record1.getValue("limited_is"));


        try(ResultSet rs = testConn.createStatement().executeQuery("select blob from data")) {
            assertTrue(rs.next());
            assertArrayEquals(bytes, rs.getBytes("blob"));
            assertFalse(rs.next());
        }
    }

    @Test
    void insertBinaryStreams() throws SQLException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, bin_stream, bin_stream_i, bin_stream_l) values (?, ?, ?, ?)");

        String text = "hello, binary stream!";
        byte[] bytes = text.getBytes();

        ps.setInt(1, 1);
        ps.setBinaryStream(2, new ByteArrayInputStream(bytes));
        ps.setBinaryStream(3, new ByteArrayInputStream(bytes), bytes.length);
        ps.setBinaryStream(4, new ByteArrayInputStream(bytes), (long)bytes.length);

        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertArrayEquals(bytes, (byte[])record1.getValue("bin_stream"));
        assertArrayEquals(bytes, (byte[])record1.getValue("bin_stream_i"));
        assertArrayEquals(bytes, (byte[])record1.getValue("bin_stream_l"));
    }



    @Test
    void insertClobs() throws SQLException, IOException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, clob, reader, limited_reader) values (?, ?, ?, ?)");

        String text = "hello, clob!";
        Clob clob = new StringClob();
        clob.setString(1, text);

        ps.setInt(1, 1);

        ps.setClob(2, clob);
        ps.setClob(3, new StringReader(text));
        ps.setClob(4, new StringReader(text), text.length());

        // Wrong clob length
        assertThrows(SQLException.class, () -> ps.setClob(4, new StringReader(text), text.length() + 1));
        assertThrows(SQLException.class, () -> ps.setClob(4, new StringReader(text), text.length() - 1));

        Reader exceptionalReader = mock(Reader.class);

        when(exceptionalReader.read(any(), anyInt(), anyInt())).thenThrow(new IOException());
        assertThrows(SQLException.class, () -> ps.setClob(4, exceptionalReader, 0));
        assertThrows(SQLException.class, () -> ps.setClob(4, exceptionalReader));

        InputStream exceptionalInputStream = mock(InputStream.class);
        when(exceptionalInputStream.read(any(), anyInt(), anyInt())).thenThrow(new IOException());
        assertThrows(SQLException.class, () -> ps.setBlob(4, exceptionalInputStream, 0));
        assertThrows(SQLException.class, () -> ps.setBlob(4, exceptionalInputStream));

        InputStream exceptionalInputStream2 = mock(InputStream.class);
        when(exceptionalInputStream2.read(any(), anyInt(), anyInt())).thenThrow(new EOFException());
        assertThrows(SQLException.class, () -> ps.setBlob(4, exceptionalInputStream2, 0));


        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertEquals(text, record1.getString("clob"));
        assertEquals(text, record1.getString("reader"));
        assertEquals(text, record1.getString("limited_reader"));
    }

    @Test
    void insertNClobs() throws SQLException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, clob, reader, limited_reader) values (?, ?, ?, ?)");

        String text = "hello, clob!";
        NClob clob = new StringClob();
        clob.setString(1, text);

        ps.setInt(1, 1);

        ps.setNClob(2, clob);
        ps.setNClob(3, new StringReader(text));
        ps.setNClob(4, new StringReader(text), text.length());

        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertEquals(text, record1.getString("clob"));
        assertEquals(text, record1.getString("reader"));
        assertEquals(text, record1.getString("limited_reader"));
    }


    @Test
    void insertAsciiStreams() throws SQLException, IOException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, ascii_stream, ascii_stream_i, ascii_stream_l) values (?, ?, ?, ?)");

        String text = "hello, ascii stream!";
        byte[] bytes = text.getBytes();
        ps.setInt(1, 1);
        ps.setAsciiStream(2, new ByteArrayInputStream(bytes));
        ps.setAsciiStream(3, new ByteArrayInputStream(bytes), text.length());
        ps.setAsciiStream(4, new ByteArrayInputStream(bytes), (long)text.length());

        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertEquals(text, record1.getString("ascii_stream"));
        assertEquals(text, record1.getString("ascii_stream_i"));
        assertEquals(text, record1.getString("ascii_stream_l"));
    }

    @Test
    void insertCharacterStreams() throws SQLException, IOException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, char_stream, char_stream_i, char_stream_l, nchar_stream, nchar_stream_l) values (?, ?, ?, ?, ?, ?)");

        String text = "hello, character stream!";
        ps.setInt(1, 1);
        ps.setCharacterStream(2, new StringReader(text));
        ps.setCharacterStream(3, new StringReader(text), text.length());
        ps.setCharacterStream(4, new StringReader(text), (long)text.length());
        ps.setNCharacterStream(5, new StringReader(text));
        ps.setNCharacterStream(6, new StringReader(text), text.length());

        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertEquals(text, record1.getString("char_stream"));
        assertEquals(text, record1.getString("char_stream_i"));
        assertEquals(text, record1.getString("char_stream_l"));
        assertEquals(text, record1.getString("nchar_stream"));
        assertEquals(text, record1.getString("nchar_stream_l"));
    }


    @Test
    void unsupportedByPreparedStatement() throws SQLException {
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, data) values (?, ?)");

        //noinspection deprecation
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setUnicodeStream(1, new ByteArrayInputStream(new byte[0], 0, 0), 0));

        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setRef(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, ps::addBatch);
        long now = currentTimeMillis();
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setDate(1, new java.sql.Date(now), Calendar.getInstance()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setTime(1, new Time(now), Calendar.getInstance()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setTimestamp(1, new Timestamp(now), Calendar.getInstance()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setRowId(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setNull(1, Types.INTEGER, "integer"));

        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setSQLXML(1, mock(SQLXML.class)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> ps.setObject(1, "", Types.VARCHAR, 0));

    }

    @Test
    void unsupportedByStatement() throws SQLException {
        Statement statement = testConn.createStatement();
        String query = "select 1";
        assertThrows(SQLException.class, () -> statement.addBatch(query)); // spec does not allow SQLFeatureNotSupportedException here
        assertThrows(SQLFeatureNotSupportedException.class, statement::clearBatch);
        assertThrows(SQLFeatureNotSupportedException.class, statement::executeBatch);
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate(query, 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate(query, new int[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.executeUpdate(query, new String[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute(query, 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute(query, new int[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.execute(query, new String[0]));

        assertFalse(statement.isPoolable());
        statement.setPoolable(false); // OK
        assertThrows(SQLException.class, () -> statement.setPoolable(true)); // spec does not allow SQLFeatureNotSupportedException here
        assertFalse(statement.isCloseOnCompletion());
        assertThrows(SQLException.class, () -> statement.setMaxFieldSize(123)); // spec does not allow SQLFeatureNotSupportedException here

        assertFalse(statement.getGeneratedKeys().next());

        assertEquals(CLOSE_CURSORS_AT_COMMIT, statement.getResultSetHoldability());
        assertThrows(SQLFeatureNotSupportedException.class, () -> statement.setCursorName("foo"));
        assertEquals(TYPE_FORWARD_ONLY, statement.getResultSetType());
        assertEquals(CONCUR_READ_ONLY, statement.getResultSetConcurrency());
        assertEquals(1, statement.getFetchSize());
        assertEquals(FETCH_FORWARD, statement.getFetchDirection());
        assertFalse(statement.isClosed());
    }


    @Test
    void insertUsingPreparedStatementAndExecute() throws SQLException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, text) values (?, ?)");
        ps.setInt(1, 1);
        ps.setString(2, "ok");
        assertTrue(ps.execute());

        Record record1 = client.get(null, key1);
        assertEquals("ok", record1.getString("text"));
    }


    @Test
    void insertWrong() throws SQLException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement("insert into data (PK, text) values (?, ?)");
        ps.setInt(1, 1);

        // Index out of bounds
        assertThrows(SQLException.class, () -> ps.setString(0, "ooops"));
        assertThrows(SQLException.class, () -> ps.setString(3, "ooops"));
        ps.setString(2, "ok");
    }


    @Test
    void insertMap() throws SQLException {
        insert("insert into data (PK, map) values (1, map('{\"one\": \"first\", \"two\": \"second\"}'))", 1);
        Record record = client.get(null, new Key("test", "data", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        Map<String, Object> map = new HashMap<>();
        map.put("one", "first");
        map.put("two", "second");
        expectedData.put("map", map);
        assertEquals(expectedData, record.bins);
    }

    @Test
    void insertArray() throws SQLException {
        insertCollection("array");
    }

    @Test
    void insertList() throws SQLException {
        insertCollection("list");
    }

    @Test
    void insertListWithMissingEntries() {
        // 0 entry is missing
        assertEquals(
                "Cannot create list due to missing entries",
                assertThrows(
                        SQLException.class,
                        () -> insert("insert into data (PK, list) values (1, list('{\"1\": \"first\", \"2\": \"second\"}'))", 1)).getMessage());
    }

    void insertCollection(String function) throws SQLException {
        insert(format("insert into data (PK, collection) values (1, %s('[1, 2, 3]'))", function), 1);
        Record record = client.get(null, new Key("test", "data", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("collection", asList(1L, 2L, 3L));
        assertEquals(expectedData, record.bins);
    }

    private String getString(Clob clob) throws SQLException {
        return clob.getSubString(1, (int)clob.length());
    }

    private byte[] getBytes(Blob blob) throws SQLException {
        return blob.getBytes(1, (int)blob.length());
    }

    private void insert(String sql, int expectedRowCount) throws SQLException {
        int rowCount = testConn.createStatement().executeUpdate(sql);
        assertEquals(expectedRowCount, rowCount);
    }
}
