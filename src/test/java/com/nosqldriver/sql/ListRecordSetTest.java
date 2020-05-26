package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static java.lang.System.currentTimeMillis;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ListRecordSetTest {
    private final String catalog = "catalog";
    private final String table = "table";

    @Test
    void noFieldsNoRecords() throws SQLException {
        ResultSet rs = new ListRecordSet(null, "schema", "table", emptyList(), emptyList());
        assertEquals(0, rs.getMetaData().getColumnCount());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.first());
        assertFalse(rs.next());
    }

    @Test
    void columnsDefinedNoRecords() throws SQLException {
        ResultSet rs = new ListRecordSet(
                null,
                catalog,
                table,
                asList(
                        DATA.create(catalog, table, "first_name", "given_name"),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth"),
                        EXPRESSION.create(catalog, table, "year()-year_of_birth", "age")),
                emptyList());


        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(3, md.getColumnCount());
        assertEquals("first_name", md.getColumnName(1));
        assertEquals("given_name", md.getColumnLabel(1));
        assertEquals("year_of_birth", md.getColumnName(2));
        assertEquals("year_of_birth", md.getColumnLabel(2));
        assertEquals("year()-year_of_birth", md.getColumnName(3));
        assertEquals("age", md.getColumnLabel(3));

        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertFalse(rs.first());
        assertFalse(rs.next());
    }


    @Test
    void noFieldsOneRecord() throws SQLException {
        ListRecordSet rs = new ListRecordSet(null, "schema", "table",
                asList(
                        DATA.create(catalog, table, "first_name", "first_name"),
                        DATA.create(catalog, table, "last_name", "last_name"),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth")),
                singletonList(asList("John", "Smith", 1970)));
        assertEquals(3, rs.getMetaData().getColumnCount());
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isFirst());
        assertFalse(rs.isLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.first());
        assertEquals("John", rs.getString(1));
        assertEquals("Smith", rs.getString(2));
        assertEquals(1970, rs.getInt(3));

        ResultSetMetaData md = rs.getMetaData();
        int n = rs.getMetaData().getColumnCount();
        Map<String, Object> actual = new HashMap<>();
        for (int i = 1; i <= n; i++) {
            actual.put(md.getColumnName(i), rs.getObject(i));
        }

        Map<String, Object> expected = new HashMap<>();
        expected.put("first_name", "John");
        expected.put("last_name", "Smith");
        expected.put("year_of_birth", 1970);
        assertEquals(expected, actual);


        assertFalse(rs.next());
    }



    @Test
    void misc() throws SQLException {
        ListRecordSet rs = new ListRecordSet(null, "schema", "table",
                asList(
                        DATA.create(catalog, table, "first_name", "first_name"),
                        DATA.create(catalog, table, "last_name", "last_name"),
                        DATA.create(catalog, table, "year_of_birth", "year_of_birth")),
                asList(asList("Neil", "Armstrong", 1930), asList("Buzz", "Aldrin", 1930)));


        assertEquals(1, rs.getFetchSize());
        rs.setFetchSize(2);
        assertEquals("Fetch size 2 (other than 1) is not supported right now and will be ignored", rs.getWarnings().getMessage());
        assertEquals(1, rs.getFetchSize());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());

        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(ResultSet.FETCH_FORWARD); //OK
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        assertThrows(SQLFeatureNotSupportedException.class, rs::previous);
    }


    @Test
    void discoverTypes() throws SQLException {
        long now = currentTimeMillis();
        List<Object> data = asList(
                (short)123, 12345, now, true, 3.14f, 3.14, "test", new byte[0],
                new java.sql.Date(currentTimeMillis()), new java.sql.Time(currentTimeMillis()),  new java.sql.Timestamp(currentTimeMillis()),
                new ArrayList<>());


        ResultSetMetaData md = new ListRecordSet(
                null,
                "schema",
                "table",
                Stream.of("short", "int", "long", "boolean", "float", "double", "string", "blob", "date", "time", "timestamp", "array")
                        .map(name -> DATA.create("schema", "talbe", name, name))
                        .collect(toList()),
                singletonList(data)).getMetaData();

        //int[] actual = ListRecordSet.discoverTypes(data.size(), singletonList(data));
        int[] actual = getTypes(md);

        int[] expected = {
                Types.SMALLINT,
                Types.INTEGER,
                Types.BIGINT,
                Types.BOOLEAN,
                Types.FLOAT,
                Types.DOUBLE,
                Types.VARCHAR,
                Types.BLOB,
                Types.DATE,
                Types.TIME,
                Types.TIMESTAMP,
                Types.ARRAY
        };

        assertArrayEquals(expected, actual);
    }


    @Test
    void discoverTypesUnknownType() throws SQLException {
        List<Object> data = singletonList(new Thread()); // Thread cannot be persisted in DB and its type cannot be discovered.
        int[] actual = getTypes(new ListRecordSet(null, null, null, singletonList(DATA.create(null, null, "thread", "thread")), singletonList(data)).getMetaData());
        assertArrayEquals(new int[] {0}, actual);
    }

    @Test
    void discoverTypesIncompatibleTypesInSameColumn() {
        assertThrows(SQLException.class, () -> discoverTypesCompatibleTypesInSameColumn(1, "1"));
    }

    @Test
    void discoverTypesCompatibleTypesInSameColumn() throws SQLException {
        discoverTypesCompatibleTypesInSameColumn(1L, 1);
    }

    @Test
    void discoverTypesCompatibleTypesInSameColumn2() throws SQLException {
        discoverTypesCompatibleTypesInSameColumn(1, 1L);
    }

    private void discoverTypesCompatibleTypesInSameColumn(Object val1, Object val2) throws SQLException {
        assertEquals(Types.BIGINT, new ListRecordSet(null, null, null, singletonList(DATA.create(null, null, "data", "data")), asList(singletonList(val1), singletonList(val2))).getMetaData().getColumnType(1));
    }

    @Test
    void updateValueListRecordSet() throws SQLException {
        assertUpdateValue(new ListRecordSet(null, "schema", "table", emptyList(), emptyList()));
    }

    @Test
    void updateValueResultSetWrapper() throws SQLException {
        assertUpdateValue(new ResultSetWrapper(new ListRecordSet(null, "schema", "table", emptyList(), emptyList()), emptyList(), true));
    }

    @Test
    void updateValueBufferedResultSet() throws SQLException {
        assertUpdateValue(new BufferedResultSet(new FilteredResultSet(new ListRecordSet(null, "schema", "table", emptyList(), emptyList()), emptyList(), r -> true, true), new ArrayList<>(), 1));
    }



    private void assertUpdateValue(ResultSet rs) throws SQLException {
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNull(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNull("field"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBoolean(1, true));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBoolean("field", true));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateByte(1, (byte)1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateByte("field", (byte)1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateShort(1, (short)1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateShort("field", (short)1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateInt(1, 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateInt("field", 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateLong(1, 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateLong("field", 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateFloat(1, 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateFloat("field", 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateDouble(1, 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateDouble("field", 1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBigDecimal(1, new BigDecimal(1)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBigDecimal("field", new BigDecimal(1)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateString(1, "value"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateString("field", "value"));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBytes(1, new byte[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBytes("field", new byte[0]));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateDate(1, new Date(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateDate("field", new Date(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateTime(1, new Time(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateTime("field", new Time(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateTimestamp(1, new java.sql.Timestamp(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateTimestamp("field", new java.sql.Timestamp(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject(1, new Object()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject("field", new Object()));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject(1, new Object(), 10));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject("field", new Object(), 10));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject(1, new Object(), JDBCType.BIGINT));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject("field", new Object(), JDBCType.BIGINT));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject(1, new Object(), JDBCType.BIGINT, 10));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateObject("field", new Object(), JDBCType.BIGINT, 10));


        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNull(1));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNull("field"));
        assertThrows(SQLFeatureNotSupportedException.class, rs::insertRow);
        assertThrows(SQLFeatureNotSupportedException.class, rs::updateRow);
        assertThrows(SQLFeatureNotSupportedException.class, rs::deleteRow);
        assertThrows(SQLFeatureNotSupportedException.class, rs::refreshRow);
        assertThrows(SQLFeatureNotSupportedException.class, rs::cancelRowUpdates);
        assertThrows(SQLFeatureNotSupportedException.class, rs::moveToInsertRow);
        assertThrows(SQLFeatureNotSupportedException.class, rs::moveToCurrentRow);
        assertThrows(SQLFeatureNotSupportedException.class, rs::rowInserted);
        assertThrows(SQLFeatureNotSupportedException.class, rs::rowUpdated);
        assertThrows(SQLFeatureNotSupportedException.class, rs::rowDeleted);
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateRef(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateRef("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBlob(1, new SerialBlob(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBlob("field", new SerialBlob(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBlob(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBlob("field", new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBlob(1, new ByteArrayInputStream(new byte[0]), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBlob("field", new ByteArrayInputStream(new byte[0]), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateClob(1, new SerialClob(new char[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateClob("field", new SerialClob(new char[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateClob(1, new StringReader("")));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateClob("field", new StringReader("")));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateClob(1, new StringReader(""), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateClob("field", new StringReader(""), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateArray(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateArray("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateRowId(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateRowId("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNString(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNString("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob(1, (NClob)null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob("field", (NClob)null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob(1, (Reader)null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob("field", (Reader)null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob(1, null, 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob("field", null, 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateSQLXML(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateSQLXML("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0]), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0]), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0]), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0]), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateCharacterStream(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateCharacterStream("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateCharacterStream(1, new StringReader(""), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateCharacterStream("field", new StringReader(""), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateCharacterStream(1, new StringReader(""), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateCharacterStream("field", new StringReader(""), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream(1, null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream("field", null));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream(1, new StringReader(""), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream("field", new StringReader(""), 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream(1, new StringReader(""), 0L));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNCharacterStream("field", new StringReader(""), 0L));

        assertThrows(SQLFeatureNotSupportedException.class, rs::previous);
        assertEquals(FETCH_FORWARD, rs.getFetchDirection());
        rs.setFetchDirection(FETCH_FORWARD);
        assertEquals(FETCH_FORWARD, rs.getFetchDirection());
//        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
        assertEquals(FETCH_FORWARD, rs.getFetchDirection());
        assertEquals(1, rs.getFetchSize());
//        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.setFetchSize(2));
        assertEquals(1, rs.getFetchSize());
        assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
    }

    private int[] getTypes(ResultSetMetaData md) throws SQLException {
        int n = md.getColumnCount();
        int[] types = new int[n];
        for (int i = 0; i < n; i++) {
            types[i] = md.getColumnType(i + 1);
        }
        return types;
    }
}