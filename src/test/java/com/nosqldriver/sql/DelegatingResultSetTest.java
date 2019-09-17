package com.nosqldriver.sql;

import com.nosqldriver.util.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.sql.Types.ARRAY;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.CLOB;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.NCLOB;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARCHAR;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelegatingResultSetTest {
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";

    private final List<DataColumn> simpleColumns = Arrays.asList(
            column("byte", TINYINT), column("short", SMALLINT), column("int", INTEGER), column("long", BIGINT),
            column("on", BOOLEAN), column("off", BOOLEAN),
            column("float", FLOAT), column("double", DOUBLE),
            column("text", VARCHAR),
            column("d", DATE), column("t", Types.TIME), column("ts", Types.TIMESTAMP),
            column("url", VARCHAR)
    );

    private final long now = currentTimeMillis();
    private final Object[] simpleRow = new Object[] {(byte)64, (short)123, 123456, now, true, false, 3.14f, 3.1415926, "hello, world!", new java.sql.Date(now), new java.sql.Time(now), new java.sql.Timestamp(now), "http://www.google.com"};
    private final List<List<?>> simpleData = singletonList(Arrays.asList(simpleRow));


    private final List<DataColumn> compositeColumns = Arrays.asList(
            column("bytes", BLOB), column("array", ARRAY), column("blob", BLOB), column("clob", CLOB), column("nclob", NCLOB)
    );
    private final Array array;
    private Blob blob = new ByteArrayBlob();
    private Clob clob = new StringClob();
    {
        try {
            array = new BasicArray(SCHEMA, "varchar", new String[] {"hello", "world"});
            blob.setBytes(1, "hello world".getBytes());
            clob.setString(1, "hello world");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    private final Object[] compositeRow = new Object[] {"hello".getBytes(), array, blob, clob, clob};
    private final List<List<?>> compositeData = singletonList(Arrays.asList(compositeRow));


    @Test
    void getAllSimpleTypesFilteredResultSet() throws SQLException, MalformedURLException {
        getAllSimpleTypes(new FilteredResultSet(new ListRecordSet("schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, true));
    }

    @Test
    void getAllSimpleTypesBufferedResultSet() throws SQLException, MalformedURLException {
        getAllSimpleTypes(new BufferedResultSet(new FilteredResultSet(new ListRecordSet("schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, true), new ArrayList<>()));
    }


    @Test
    void getAllCompositeTypesFilteredResultSet() throws SQLException, IOException {
        getAllCompositeTypes(new FilteredResultSet(new ListRecordSet("schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, true));
    }

    @Test
    void getAllCompositeTypesBufferedResultSet() throws SQLException, IOException {
        getAllCompositeTypes(new BufferedResultSet(new FilteredResultSet(new ListRecordSet("schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, true), new ArrayList<>()));
    }



    void getAllSimpleTypes(ResultSet rs) throws SQLException, MalformedURLException {
        assertTrue(rs.next());

        assertEquals(simpleRow[0], rs.getByte(1));
        assertEquals(simpleRow[0], rs.getByte("byte"));
        Byte expByte = (byte) simpleRow[0];
        assertEquals(expByte.shortValue(), rs.getShort(1));
        assertEquals(expByte.shortValue(), rs.getShort("byte"));
        assertEquals(expByte.intValue(), rs.getInt(1));
        assertEquals(expByte.intValue(), rs.getInt("byte"));
        assertEquals(expByte.longValue(), rs.getLong(1));
        assertEquals(expByte.longValue(), rs.getLong("byte"));

        assertEquals(simpleRow[1], rs.getShort(2));
        assertEquals(simpleRow[1], rs.getShort("short"));
        Short expShort = (short) simpleRow[1];
        assertEquals(expShort.intValue(), rs.getInt(2));
        assertEquals(expShort.intValue(), rs.getInt("short"));
        assertEquals(expShort.longValue(), rs.getLong(2));
        assertEquals(expShort.longValue(), rs.getLong("short"));

        assertEquals(simpleRow[2], rs.getInt(3));
        assertEquals(simpleRow[2], rs.getInt("int"));
        assertEquals(Integer.valueOf((int) simpleRow[2]).longValue(), rs.getLong(3));
        assertEquals(Integer.valueOf((int) simpleRow[2]).longValue(), rs.getLong("int"));

        assertEquals(simpleRow[3], rs.getLong(4));
        assertEquals(simpleRow[3], rs.getLong("long"));

        assertEquals(simpleRow[4], rs.getBoolean(5));
        assertEquals(simpleRow[4], rs.getBoolean("on"));
        assertEquals(simpleRow[5], rs.getBoolean(6));
        assertEquals(simpleRow[5], rs.getBoolean("off"));

        assertEquals((float) simpleRow[6], rs.getFloat(7));
        assertEquals((float) simpleRow[6], rs.getFloat("float"));
        assertEquals((float) simpleRow[6], rs.getDouble(7));
        assertEquals((float) simpleRow[6], rs.getDouble("float"));

        assertEquals((double) simpleRow[7], rs.getDouble(8));
        assertEquals((double) simpleRow[7], rs.getDouble("double"));
        assertEquals((double) simpleRow[7], rs.getBigDecimal(8).doubleValue());
        assertEquals((double) simpleRow[7], rs.getBigDecimal("double").doubleValue());



        assertEquals(simpleRow[8], rs.getString(9));
        assertEquals(simpleRow[8], rs.getString("text"));

        assertEquals(simpleRow[9], rs.getDate(10));
        assertEquals(simpleRow[9], rs.getDate("d"));

        assertEquals(simpleRow[10], rs.getTime(11));
        assertEquals(simpleRow[10], rs.getTime("t"));

        assertEquals(simpleRow[11], rs.getTimestamp(12));
        assertEquals(simpleRow[11], rs.getTimestamp("ts"));

        URL expUrl = new URL((String) simpleRow[12]);
        assertEquals(expUrl, rs.getURL(13));
        assertEquals(expUrl, rs.getURL("url"));

        assertFalse(rs.next());
    }




    void getAllCompositeTypes(ResultSet rs) throws SQLException, IOException {
        assertTrue(rs.next());


        assertArrayEquals((byte[])compositeRow[0], rs.getBytes(1));
        assertArrayEquals((byte[])compositeRow[0], rs.getBytes("bytes"));
        assertArrayEquals((byte[])compositeRow[0], IOUtils.toByteArray(rs.getBinaryStream(1)));
        assertArrayEquals((byte[])compositeRow[0], IOUtils.toByteArray(rs.getBinaryStream("bytes")));

        assertEquals(compositeRow[1], rs.getArray(2));
        assertEquals(compositeRow[1], rs.getArray("array"));

        assertEquals(compositeRow[2], rs.getBlob(3));
        assertEquals(compositeRow[2], rs.getBlob("blob"));

        byte[] expBytes = IOUtils.toByteArray(((Blob)compositeRow[2]).getBinaryStream());
        assertArrayEquals(expBytes, IOUtils.toByteArray(rs.getBinaryStream(3)));
        assertArrayEquals(expBytes, IOUtils.toByteArray(rs.getBinaryStream("blob")));


        String expClob = IOUtils.toString(((Clob)compositeRow[3]).getCharacterStream());
        assertEquals(expClob, IOUtils.toString(rs.getClob(4).getCharacterStream()));
        assertEquals(expClob, IOUtils.toString(rs.getClob("clob").getCharacterStream()));
        assertEquals(expClob, IOUtils.toString(rs.getNClob(4).getCharacterStream()));
        assertEquals(expClob, IOUtils.toString(rs.getNClob("clob").getCharacterStream()));

        assertEquals(expClob, IOUtils.toString(rs.getCharacterStream(4)));
        assertEquals(expClob, IOUtils.toString(rs.getCharacterStream("clob")));
        assertEquals(expClob, IOUtils.toString(rs.getNCharacterStream(4)));
        assertEquals(expClob, IOUtils.toString(rs.getNCharacterStream("clob")));

        String expNClob = IOUtils.toString(((Clob)compositeRow[4]).getCharacterStream());
        assertEquals(expNClob, IOUtils.toString(rs.getClob(5).getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getClob("nclob").getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getNClob(5).getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getNClob("nclob").getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getCharacterStream(5)));
        assertEquals(expNClob, IOUtils.toString(rs.getCharacterStream("nclob")));
        assertEquals(expNClob, IOUtils.toString(rs.getNCharacterStream(5)));
        assertEquals(expNClob, IOUtils.toString(rs.getNCharacterStream("nclob")));

        assertFalse(rs.next());
    }

    @Test
    void unsupported() {
        ResultSet rs = new BufferedResultSet(new FilteredResultSet(new ListRecordSet("schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, true), new ArrayList<>());
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getObject(1, Collections.emptyMap()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getObject("any", Collections.emptyMap()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getRef(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getRef("any"));
    }


    private DataColumn column(String name, int type) {
        return DataColumn.DataColumnRole.DATA.create(SCHEMA, TABLE, name, name).withType(type);
    }
}