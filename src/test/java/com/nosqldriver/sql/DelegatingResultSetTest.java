package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.util.IOUtils;
import com.nosqldriver.util.VariableSource;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
import java.util.Calendar;
import java.util.List;
import java.util.stream.Stream;

import static java.lang.String.format;
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
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelegatingResultSetTest {
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";

    private static final List<DataColumn> simpleColumns = asList(
            column("byte", TINYINT), column("short", SMALLINT), column("int", INTEGER), column("long", BIGINT),
            column("on", BOOLEAN), column("off", BOOLEAN),
            column("float", FLOAT), column("double", DOUBLE),
            column("text", VARCHAR),
            column("d", DATE), column("t", Types.TIME), column("ts", Types.TIMESTAMP),
            column("url", VARCHAR),
            column("empty", Types.NULL)
    );

    private static final long now = currentTimeMillis();
    private static final Object[] simpleRow = new Object[] {(byte)64, (short)123, 123456, now, true, false, 3.14f, 3.1415926, "hello, world!", new java.sql.Date(now), new java.sql.Time(now), new java.sql.Timestamp(now), "http://www.google.com", null};
    private static final List<List<?>> simpleData = singletonList(asList(simpleRow));


    private static final List<DataColumn> compositeColumns = asList(
            column("bytes", BLOB), column("array", ARRAY), column("blob", BLOB), column("clob", CLOB), column("nclob", NCLOB)
    );
    private static final Array array;
    private static Blob blob = new ByteArrayBlob();
    private static Clob clob = new StringClob();
    static {
        try {
            array = new BasicArray(SCHEMA, "varchar", new String[] {"hello", "world"});
            blob.setBytes(1, "hello world".getBytes());
            clob.setString(1, "hello world");
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    private static final Object[] compositeRow = new Object[] {"hello".getBytes(), array, blob, clob, clob};
    private static final List<List<?>> compositeData = singletonList(asList(compositeRow));

    @VisibleForPackage // visible for tests
    @SuppressWarnings("unused") // referenced from annotation VariableSource
    private static Stream<Arguments> resultSetsForCompositeTypes = Stream.of(
            Arguments.of(new BufferedResultSet(new FilteredResultSet(new ListRecordSet(null, "schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, true), new ArrayList<>(), 0), "Buffered(Filtered(index-by-name))"),
            Arguments.of(new BufferedResultSet(new FilteredResultSet(new ListRecordSet(null, "schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, false), new ArrayList<>(), 0), "Buffered(Filtered)"),
            Arguments.of(new FilteredResultSet(new ListRecordSet(null, "schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, true), "Filtered(index-by-name)"),
            Arguments.of(new FilteredResultSet(new ListRecordSet(null, "schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, false), "Filtered")
    );

    @VisibleForPackage // visible for tests
    @SuppressWarnings("unused") // referenced from annotation VariableSource
    private static Stream<Arguments> resultSetsForSimpleTypes = Stream.of(
            Arguments.of(new BufferedResultSet(new FilteredResultSet(new ListRecordSet(null, "schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, true), new ArrayList<>(), 0), "Buffered(Filtered(index-by-name))"),
            Arguments.of(new FilteredResultSet(new ListRecordSet(null, "schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, true), "Filtered(index-by-name)"),
            Arguments.of(new FilteredResultSet(new ListRecordSet(null, "schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, false), "Filtered")
    );

    @VisibleForPackage // visible for tests
    @SuppressWarnings("unused") // referenced from annotation VariableSource
    private static Stream<Arguments> resultSetsForUnsupported = Stream.of(
            Arguments.of(new FilteredResultSet(new ListRecordSet(null, "schema", "table", simpleColumns, simpleData), simpleColumns, r -> true, true), "Filtered(index-by-name)"),
            Arguments.of(new BufferedResultSet(new FilteredResultSet(new ListRecordSet(null, "schema", "table", compositeColumns, compositeData), compositeColumns, r -> true, true), new ArrayList<>(), 0), "Buffered(Filtered(index-by-name))"),
            Arguments.of(new JoinedResultSet(new ListRecordSet(null, "schema", "table", simpleColumns, simpleData), emptyList()), "Joined")
    );


    @ParameterizedTest(name = "{index} {1}")
    @VariableSource("resultSetsForSimpleTypes")
    void getAllSimpleTypes(ResultSet rs, String name) throws SQLException, MalformedURLException {
        assertEquals(0, rs.getRow());
        assertTrue(rs.next());
        assertEquals(1, rs.getRow());
        assertTrue(rs.isFirst());
        //assertTrue(rs.isLast()); //FIXME does not work for filtered RS

        assertEquals(simpleRow[0], rs.getByte(1));
        assertEquals(simpleRow[0], rs.getByte("byte"));
        Byte expByte = (byte) simpleRow[0];
        assertEquals(expByte.shortValue(), rs.getShort(1));
        assertEquals(expByte.shortValue(), rs.getShort("byte"));
        assertEquals(expByte.intValue(), rs.getInt(1));
        assertEquals(expByte.intValue(), rs.getInt("byte"));
        assertEquals(expByte.longValue(), rs.getLong(1));
        assertEquals(expByte.longValue(), rs.getLong("byte"));
        assertEquals(format("%d cannot be transformed to byte[]", expByte.intValue()), assertThrows(SQLException.class, () -> rs.getBytes(1)).getMessage());
        assertFalse(rs.wasNull());

        assertEquals(simpleRow[1], rs.getShort(2));
        assertEquals(simpleRow[1], rs.getShort("short"));
        Short expShort = (short) simpleRow[1];
        assertEquals(expShort.intValue(), rs.getInt(2));
        assertEquals(expShort.intValue(), rs.getInt("short"));
        assertEquals(expShort.longValue(), rs.getLong(2));
        assertEquals(expShort.longValue(), rs.getLong("short"));
        assertFalse(rs.wasNull());

        assertEquals(simpleRow[2], rs.getInt(3));
        assertEquals(simpleRow[2], rs.getInt("int"));
        assertEquals(Integer.valueOf((int) simpleRow[2]).longValue(), rs.getLong(3));
        assertEquals(Integer.valueOf((int) simpleRow[2]).longValue(), rs.getLong("int"));
        assertFalse(rs.wasNull());

        assertEquals(simpleRow[3], rs.getLong(4));
        assertEquals(simpleRow[3], rs.getLong("long"));

        assertEquals(simpleRow[3], rs.getDate(4).getTime());
        assertEquals(simpleRow[3], rs.getDate("long").getTime());
        assertEquals(simpleRow[3], rs.getTime(4).getTime());
        assertEquals(simpleRow[3], rs.getTime("long").getTime());
        assertEquals(simpleRow[3], rs.getTimestamp(4).getTime());
        assertEquals(simpleRow[3], rs.getTimestamp("long").getTime());

        Calendar calendar = Calendar.getInstance();
        assertEquals(simpleRow[3], rs.getDate(4, calendar).getTime());
        assertEquals(simpleRow[3], rs.getDate("long", calendar).getTime());
        assertEquals(simpleRow[3], rs.getTime(4, calendar).getTime());
        assertEquals(simpleRow[3], rs.getTime("long", calendar).getTime());
        assertEquals(simpleRow[3], rs.getTimestamp(4, calendar).getTime());
        assertEquals(simpleRow[3], rs.getTimestamp("long", calendar).getTime());


        assertEquals(simpleRow[4], rs.getBoolean(5));
        assertEquals(simpleRow[4], rs.getBoolean("on"));
        assertEquals(simpleRow[5], rs.getBoolean(6));
        assertEquals(simpleRow[5], rs.getBoolean("off"));
        assertFalse(rs.wasNull());

        assertEquals((float) simpleRow[6], rs.getFloat(7));
        assertEquals((float) simpleRow[6], rs.getFloat("float"));
        assertEquals((float) simpleRow[6], rs.getDouble(7));
        assertEquals((float) simpleRow[6], rs.getDouble("float"));
        assertFalse(rs.wasNull());

        assertEquals((double) simpleRow[7], rs.getDouble(8));
        assertEquals((double) simpleRow[7], rs.getDouble("double"));
        assertEquals((double) simpleRow[7], rs.getBigDecimal(8).doubleValue());
        assertEquals((double) simpleRow[7], rs.getBigDecimal("double").doubleValue());

        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getBinaryStream(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getBinaryStream("double")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getAsciiStream(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getAsciiStream("double")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getCharacterStream(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getCharacterStream("double")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getNCharacterStream(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getNCharacterStream("double")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getUnicodeStream(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getUnicodeStream("double")));

        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getBlob(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getBlob("double")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getClob(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getClob("double")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getNClob(8)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[7], rs.getNClob("double")));

        assertFalse(rs.wasNull());

        double roundedExpValue = new BigDecimal((double) simpleRow[7]).setScale(2, RoundingMode.FLOOR).doubleValue();
        assertEquals(roundedExpValue, rs.getBigDecimal(8, 2).doubleValue());
        assertEquals(roundedExpValue, rs.getBigDecimal("double", 2).doubleValue());
        assertFalse(rs.wasNull());


        assertEquals(simpleRow[8], rs.getString(9));
        assertEquals(simpleRow[8], rs.getString("text"));
        assertEquals(simpleRow[8], rs.getNString(9));
        assertEquals(simpleRow[8], rs.getNString("text"));

        assertThrows(SQLException.class, () -> assertEquals(simpleRow[8], rs.getURL(9)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[8], rs.getURL("text")));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[8], rs.getInt(9)));
        assertThrows(SQLException.class, () -> assertEquals(simpleRow[8], rs.getInt("text")));


        assertFalse(rs.wasNull());

        assertEquals(simpleRow[9], rs.getDate(10));
        assertEquals(simpleRow[9], rs.getDate("d"));
        assertFalse(rs.wasNull());

        assertEquals(simpleRow[10], rs.getTime(11));
        assertEquals(simpleRow[10], rs.getTime("t"));
        assertFalse(rs.wasNull());

        assertEquals(simpleRow[11], rs.getTimestamp(12));
        assertEquals(simpleRow[11], rs.getTimestamp("ts"));
        assertFalse(rs.wasNull());

        URL expUrl = new URL((String) simpleRow[12]);
        assertEquals(expUrl, rs.getURL(13));
        assertEquals(expUrl, rs.getURL("url"));
        assertEquals(expUrl, rs.getObject("url", URL.class));
        assertFalse(rs.wasNull());


        assertNull(rs.getObject(14));
        assertNull(rs.getObject("empty"));
        assertTrue(rs.wasNull());

        assertEquals(1, rs.getRow());

        assertFalse(rs.next());
    }


    @ParameterizedTest(name = "{1}")
    @VariableSource("resultSetsForCompositeTypes")
    void getAllCompositeTypes(ResultSet rs, String name) throws SQLException, IOException {
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertTrue(rs.next());

        assertTrue(rs.isFirst());
        //assertTrue(rs.isLast()); // FIXME does not work for filtered RS

        assertArrayEquals((byte[])compositeRow[0], rs.getBytes(1));
        assertArrayEquals((byte[])compositeRow[0], rs.getBytes("bytes"));
        assertArrayEquals((byte[])compositeRow[0], IOUtils.toByteArray(rs.getBinaryStream(1)));
        assertArrayEquals((byte[])compositeRow[0], IOUtils.toByteArray(rs.getBinaryStream("bytes")));
        assertArrayEquals((byte[])compositeRow[0], IOUtils.toByteArray(rs.getBlob(1).getBinaryStream()));
        assertArrayEquals((byte[])compositeRow[0], IOUtils.toByteArray(rs.getBlob("bytes").getBinaryStream()));


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
        assertEquals(expClob, new String(IOUtils.toByteArray(rs.getAsciiStream(4))));
        assertEquals(expClob, new String(IOUtils.toByteArray(rs.getAsciiStream("clob"))));
        assertEquals(expClob, new String(IOUtils.toByteArray(rs.getUnicodeStream(4))));
        assertEquals(expClob, new String(IOUtils.toByteArray(rs.getUnicodeStream("clob"))));


        String expNClob = IOUtils.toString(((Clob)compositeRow[4]).getCharacterStream());
        assertEquals(expNClob, IOUtils.toString(rs.getClob(5).getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getClob("nclob").getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getNClob(5).getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getNClob("nclob").getCharacterStream()));
        assertEquals(expNClob, IOUtils.toString(rs.getCharacterStream(5)));
        assertEquals(expNClob, IOUtils.toString(rs.getCharacterStream("nclob")));
        assertEquals(expNClob, IOUtils.toString(rs.getNCharacterStream(5)));
        assertEquals(expNClob, IOUtils.toString(rs.getNCharacterStream("nclob")));
        assertEquals(expNClob, new String(IOUtils.toByteArray(rs.getAsciiStream(5))));
        assertEquals(expNClob, new String(IOUtils.toByteArray(rs.getAsciiStream("nclob"))));

        assertFalse(rs.next());
        //assertFalse(rs.isFirst()); //FIXME does not work for filtered
        assertTrue(rs.isAfterLast());
    }


    @ParameterizedTest(name = "{1}")
    @VariableSource("resultSetsForUnsupported")
    void unsupported(ResultSet rs, String name) throws SQLException {
        assertTrue(rs.first());
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getObject(1, emptyMap()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getObject("any", emptyMap()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getRef(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getRef("any"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getRowId(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getRowId("any"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getSQLXML(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.getSQLXML("any"));
        assertThrows(SQLFeatureNotSupportedException.class, rs::getCursorName);
    }


    private static DataColumn column(String name, int type) {
        return DataColumn.DataColumnRole.DATA.create(SCHEMA, TABLE, name, name).withType(type);
    }
}