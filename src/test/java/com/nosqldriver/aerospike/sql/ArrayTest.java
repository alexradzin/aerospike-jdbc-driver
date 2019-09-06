package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.client;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.testConn;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrayTest {
    private static final String DATA = "data";

    @BeforeEach
    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, DATA);
    }

    @Test
    void insertOneRowUsingPreparedStatement() throws SQLException {
        PreparedStatement insert = testConn.prepareStatement("insert into people (PK, id, first_name, last_name, kids) values (?, ?, ?, ?, ?)");
        insert.setInt(1, 1);
        insert.setInt(2, 1);
        insert.setString(3, "John");
        insert.setString(4, "Lennon");
        insert.setArray(5, testConn.createArrayOf("varchar", new String[] {"Sean", "Julian"}));
        int rowCount = insert.executeUpdate();
        assertEquals(1, rowCount);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("kids", Arrays.asList("Sean", "Julian"));
        assertEquals(expectedData, record.bins);


        PreparedStatement select = testConn.prepareStatement("select id, first_name, last_name, kids from people where PK=?");
        select.setInt(1, 1);
        ResultSet rs = select.executeQuery();
        assertTrue(rs.next());

        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("id"));
        assertEquals("John", rs.getString(2));
        assertEquals("John", rs.getString("first_name"));
        assertEquals("Lennon", rs.getString(3));
        assertEquals("Lennon", rs.getString("last_name"));

        List<String> expectedKids = Arrays.asList("Sean", "Julian");
        assertEquals(expectedKids, rs.getObject(4));
        assertEquals(expectedKids, rs.getObject("kids"));

        assertEquals(expectedKids, Arrays.asList((Object[])rs.getArray(4).getArray()));
        assertEquals(expectedKids, Arrays.asList((Object[])rs.getArray("kids").getArray()));


        ResultSet arrayRs = rs.getArray(4).getResultSet();
        assertTrue(arrayRs.next());
        assertEquals(1, arrayRs.getInt(1));
        assertEquals("Sean", arrayRs.getString(2));
        assertTrue(arrayRs.next());
        assertEquals(2, arrayRs.getInt(1));
        assertEquals("Julian", arrayRs.getString(2));
        assertFalse(arrayRs.next());


        assertFalse(rs.next());
    }



    @Test
    void insertOneRowUsingPreparedStatementVariousArrayTypes() throws SQLException {
        Key key1 = new Key(NAMESPACE, DATA, 1);
        assertNull(client.get(null, key1));
        PreparedStatement ps = testConn.prepareStatement(
                "insert into data (PK, byte, short, int, long, boolean, string, nstring, blob, clob, nclob, t, ts, d) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        );

        long now = currentTimeMillis();
        String helloWorld = "hello, world!";
        ps.setInt(1, 1);
        ps.setObject(2, new byte[] {(byte)8});
        ps.setObject(3, new short[] {(short)512});
        ps.setObject(4, new int[] {1024});
        ps.setObject(5, new long[] {now});
        ps.setObject(6, new boolean[] {true});
        ps.setObject(7, new String[] {"hello"});
        ps.setObject(8, new String[] {helloWorld});

        Blob blob = testConn.createBlob();
        blob.setBytes(1, helloWorld.getBytes());
        ps.setObject(9, new Blob[] {blob});

        Clob clob = testConn.createClob();
        clob.setString(1, helloWorld);
        ps.setObject(10, new Clob[] {clob});

        NClob nclob = testConn.createNClob();
        nclob.setString(1, helloWorld);
        ps.setObject(11, new NClob[] {nclob});

        ps.setObject(12, new Time[] {new Time(now)});
        ps.setObject(13, new Timestamp[] {new Timestamp(now)});
        ps.setObject(14, new java.sql.Date[] {new java.sql.Date(now)});


        assertEquals(1, ps.executeUpdate());

        Record record1 = client.get(null, key1);
        assertNotNull(record1);
        assertArrayEquals(new byte[]{8}, (byte[])record1.getValue("byte")); // byte array is special case since it is stored as byte array
        // int primitives (short, int, long) are stored as longs in Aerospike
        assertEquals(new ArrayList<>(singleton(512L)), record1.getList("short"));
        assertEquals(new ArrayList<>(singleton(1024L)), record1.getList("int"));
        assertEquals(new ArrayList<>(singleton(now)), record1.getList("long"));
        assertEquals(new ArrayList<>(singleton(true)), record1.getList("boolean"));
        assertEquals(new ArrayList<>(singleton("hello")), record1.getList("string"));
        assertEquals(new ArrayList<>(singleton(helloWorld)), record1.getList("nstring"));

        List<?> actualBlob = record1.getList("blob");
        assertEquals(1, actualBlob.size());
        assertArrayEquals(helloWorld.getBytes(), (byte[])actualBlob.get(0));


        assertEquals(new ArrayList<>(singleton(helloWorld)), record1.getList("clob"));
        assertEquals(new ArrayList<>(singleton(helloWorld)), record1.getList("nclob"));
        assertEquals(new ArrayList<>(singleton(new Time(now))), record1.getList("t"));
        assertEquals(new ArrayList<>(singleton(new Timestamp(now))), record1.getList("ts"));
        assertEquals(new ArrayList<>(singleton(new java.sql.Date(now))), record1.getList("d"));


        PreparedStatement query = testConn.prepareStatement("select byte, short, int, long, boolean, string, nstring, blob, clob, nclob, t, ts, d from data where PK=?");
        query.setInt(1, 1);

        ResultSet rs = query.executeQuery();
        ResultSetMetaData md = rs.getMetaData();

        int n = md.getColumnCount();

        for (int i = 1; i <= n; i++) {
            assertEquals(DATA, md.getTableName(i));
            assertEquals(NAMESPACE, md.getCatalogName(i));
        }

        // All ints except short are  stored as longs
        assertEquals(Types.BLOB, md.getColumnType(1)); // byte array is converted to blob
        for (int i = 2; i <= n; i++) {
            assertEquals(Types.ARRAY, md.getColumnType(i), format("Wrong type of column %d: expected ARRAY(%d) but was %d", i, Types.ARRAY, md.getColumnType(i)));
        }

        assertTrue(rs.first());


        byte[] expectedBytes = {8};
        assertArrayEquals(expectedBytes, rs.getBytes(1));
        assertArrayEquals(expectedBytes, rs.getBytes("byte"));
        assertArrayEquals(expectedBytes, getBytes(rs.getBlob(1)));
        assertArrayEquals(expectedBytes, getBytes(rs.getBlob("byte")));


        Long[] expectedShorts = {512L};
        assertArrayEquals(expectedShorts, (Object[])rs.getArray(2).getArray());
        assertArrayEquals(expectedShorts, (Object[])rs.getArray("short").getArray());

        Long[] expectedInts = {1024L};
        assertArrayEquals(expectedInts, (Object[])rs.getArray(3).getArray());
        assertArrayEquals(expectedInts, (Object[])rs.getArray("int").getArray());

        Long[] expectedLongs = {now};
        assertArrayEquals(expectedLongs, (Object[])rs.getArray(4).getArray());
        assertArrayEquals(expectedLongs, (Object[])rs.getArray("long").getArray());


        Boolean[] expectedBooleans = {true};
        assertArrayEquals(expectedBooleans, (Object[])rs.getArray(5).getArray());
        assertArrayEquals(expectedBooleans, (Object[])rs.getArray("boolean").getArray());

        String[] expectedStrings = {"hello"};
        assertArrayEquals(expectedStrings, (Object[])rs.getArray(6).getArray());
        assertArrayEquals(expectedStrings, (Object[])rs.getArray("string").getArray());


        String[] expectedNStrings = {helloWorld};
        assertArrayEquals(expectedNStrings, (Object[])rs.getArray(7).getArray());
        assertArrayEquals(expectedNStrings, (Object[])rs.getArray("nstring").getArray());


        byte[][] expectedBlobs = {helloWorld.getBytes()};

        assertArrayEquals(expectedBlobs, Arrays.stream(((Object[]) rs.getArray(8).getArray())).map(b -> getBytes((Blob)b)).toArray());
        assertArrayEquals(expectedBlobs, Arrays.stream(((Object[]) rs.getArray("blob").getArray())).map(b -> getBytes((Blob)b)).toArray());

        // no way to distinguish between clob and string when rading
        assertArrayEquals(expectedNStrings, Arrays.stream(((Object[]) rs.getArray(9).getArray())).toArray());
        assertArrayEquals(expectedNStrings, Arrays.stream(((Object[]) rs.getArray("clob").getArray())).toArray());

        assertArrayEquals(expectedNStrings, Arrays.stream(((Object[]) rs.getArray(10).getArray())).toArray());
        assertArrayEquals(expectedNStrings, Arrays.stream(((Object[]) rs.getArray("nclob").getArray())).toArray());


        Time[] expectedTimes = {new Time(now)};
        assertArrayEquals(expectedTimes, (Object[])rs.getArray(11).getArray());
        assertArrayEquals(expectedTimes, (Object[])rs.getArray("t").getArray());

        Time[] expectedTimestamps = {new Time(now)};
        assertArrayEquals(expectedTimestamps, (Object[])rs.getArray(12).getArray());
        assertArrayEquals(expectedTimestamps, (Object[])rs.getArray("ts").getArray());

        java.sql.Date[] expectedDates = {new java.sql.Date(now)};
        assertArrayEquals(expectedDates, (Object[])rs.getArray(12).getArray());
        assertArrayEquals(expectedDates, (Object[])rs.getArray("d").getArray());

        assertFalse(rs.next());
    }

    private byte[] getBytes(Blob blob) {
        try {
            return blob.getBytes(1, (int)blob.length());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
