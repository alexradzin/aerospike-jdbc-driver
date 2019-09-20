package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class ListRecordSetTest {
    private final String catalog = "catalog";
    private final String table = "table";

    @Test
    void noFieldsNoRecords() throws SQLException {
        ResultSet rs = new ListRecordSet("schema", "table", emptyList(), emptyList());
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
        ResultSet rs = new ListRecordSet("schema", "table",
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
        assertFalse(rs.next());
    }

    @Test
    void discoverTypes() {
        long now = currentTimeMillis();
        List<Object> data = asList(
                (short)123, 12345, now, true, 3.14f, 3.14, "test", new byte[0],
                new java.sql.Date(currentTimeMillis()), new java.sql.Time(currentTimeMillis()),  new java.sql.Timestamp(currentTimeMillis()),
                new ArrayList<>());

        int[] actual = ListRecordSet.discoverTypes(data.size(), singletonList(data));
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
    void updateValueListRecordSet() {
        assertUpdateValue(new ListRecordSet("schema", "table", emptyList(), emptyList()));
    }

    @Test
    void updateValueResultSetWrapper() {
        assertUpdateValue(new ResultSetWrapper(new ListRecordSet("schema", "table", emptyList(), emptyList()), emptyList(), true));
    }

    @Test
    void updateValueBufferedResultSet() {
        assertUpdateValue(new BufferedResultSet(new FilteredResultSet(new ListRecordSet("schema", "table", emptyList(), emptyList()), emptyList(), r -> true, true), new ArrayList<>()));
    }

    @Test
    void updateValueResultSetWrapperWithMock() throws SQLException {
        ResultSet mock = Mockito.mock(ResultSet.class);
        ResultSet rs = new ResultSetWrapper(mock, emptyList(), true);

        rs.updateNull(1);
        rs.updateNull("field");

        rs.updateBoolean(1, true);
        rs.updateBoolean("field", true);
        rs.updateByte(1, (byte)1);
        rs.updateByte("field", (byte)1);
        rs.updateShort(1, (short)1);
        rs.updateShort("field", (short)1);
        rs.updateInt(1, 1);
        rs.updateInt("field", 1);
        rs.updateLong(1, 1);
        rs.updateLong("field", 1);
        rs.updateFloat(1, 1);
        rs.updateFloat("field", 1);
        rs.updateDouble(1, 1);
        rs.updateDouble("field", 1);
        rs.updateBigDecimal(1, new BigDecimal(1));
        rs.updateBigDecimal("field", new BigDecimal(1));
        rs.updateString(1, "value");
        rs.updateString("field", "value");
        rs.updateBytes(1, new byte[0]);
        rs.updateBytes("field", new byte[0]);
        rs.updateDate(1, new Date(0));
        rs.updateDate("field", new Date(0));
        rs.updateTime(1, new Time(0));
        rs.updateTime("field", new Time(0));
        rs.updateTimestamp(1, new Timestamp(0));
        rs.updateTimestamp("field", new Timestamp(0));
        rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]));
        rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0]));
        rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]));
        rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0]));
        rs.updateObject(1, new Object());
        rs.updateObject("field", new Object());
        rs.updateObject(1, new Object(), 10);
        rs.updateObject("field", new Object(), 10);
//        rs.updateObject(1, new Object(), JDBCType.BIGINT);
//        rs.updateObject("field", new Object(), JDBCType.BIGINT);
//        rs.updateObject(1, new Object(), JDBCType.BIGINT, 10);
//        rs.updateObject("field", new Object(), JDBCType.BIGINT, 10);


//        rs.updateNull(1);
//        rs.updateNull("field");
        rs.insertRow();
        rs.updateRow();
        rs.deleteRow();
        rs.refreshRow();
        rs.cancelRowUpdates();
        rs.moveToInsertRow();
        rs.moveToCurrentRow();
        rs.rowInserted();
        rs.rowUpdated();
        rs.rowDeleted();

        rs.updateRef(1, null);
        rs.updateRef("field", null);
        rs.updateBlob(1, new SerialBlob(new byte[0]));
        rs.updateBlob("field", new SerialBlob(new byte[0]));
        rs.updateBlob(1, new ByteArrayInputStream(new byte[0]));
        rs.updateBlob("field", new ByteArrayInputStream(new byte[0]));
        rs.updateBlob(1, new ByteArrayInputStream(new byte[0]), 0);
        rs.updateBlob("field", new ByteArrayInputStream(new byte[0]), 0);
        rs.updateClob(1, new SerialClob(new char[0]));
        rs.updateClob("field", new SerialClob(new char[0]));
        rs.updateClob(1, new StringReader(""));
        rs.updateClob("field", new StringReader(""));
        rs.updateClob(1, new StringReader(""), 0);
        rs.updateClob("field", new StringReader(""), 0);
        rs.updateArray(1, null);
        rs.updateArray("field", null);
        rs.updateRowId(1, null);
        rs.updateRowId("field", null);
        rs.updateNString(1, null);
        rs.updateNString("field", null);
        rs.updateNClob(1, (NClob)null);
        rs.updateNClob("field", (NClob)null);
        rs.updateNClob(1, (Reader)null);
        rs.updateNClob("field", (Reader)null);
        rs.updateNClob(1, (Reader)null, 0);
        rs.updateNClob("field", (Reader)null, 0);
        rs.updateSQLXML(1, null);
        rs.updateSQLXML("field", null);
        rs.updateAsciiStream(1, null);
        rs.updateAsciiStream("field", null);
        rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 0);
        rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0]), 0);
        rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0]), 0L);
        rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0]), 0L);
        rs.updateBinaryStream(1, null);
        rs.updateBinaryStream("field", null);

        rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0);
        rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0]), 0);

        rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0L);
        rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0]), 0L);
        rs.updateCharacterStream(1, null);
        rs.updateCharacterStream("field", null);
        rs.updateCharacterStream(1, new StringReader(""), 0);
        rs.updateCharacterStream("field", new StringReader(""), 0);
        rs.updateCharacterStream(1, new StringReader(""), 0L);
        rs.updateCharacterStream("field", new StringReader(""), 0L);
        rs.updateNCharacterStream(1, null);
        rs.updateNCharacterStream("field", null);
        rs.updateNCharacterStream(1, new StringReader(""), 0L);
        rs.updateNCharacterStream("field", new StringReader(""), 0L);



        verify(mock, times(1)).updateNull(1);
        verify(mock, times(1)).updateNull("field");

        verify(mock, times(1)).updateBoolean(1, true);
        verify(mock, times(1)).updateBoolean("field", true);
        verify(mock, times(1)).updateByte(1, (byte)1);
        verify(mock, times(1)).updateByte("field", (byte)1);
        verify(mock, times(1)).updateShort(1, (short)1);
        verify(mock, times(1)).updateShort("field", (short)1);
        verify(mock, times(1)).updateInt(1, 1);
        verify(mock, times(1)).updateInt("field", 1);
        verify(mock, times(1)).updateLong(1, 1);
        verify(mock, times(1)).updateLong("field", 1);
        verify(mock, times(1)).updateFloat(1, 1);
        verify(mock, times(1)).updateFloat("field", 1);
        verify(mock, times(1)).updateDouble(1, 1);
        verify(mock, times(1)).updateDouble("field", 1);
        verify(mock, times(1)).updateBigDecimal(1, new BigDecimal(1));
        verify(mock, times(1)).updateBigDecimal("field", new BigDecimal(1));
        verify(mock, times(1)).updateString(1, "value");
        verify(mock, times(1)).updateString("field", "value");
        verify(mock, times(1)).updateBytes(1, new byte[0]);
        verify(mock, times(1)).updateBytes("field", new byte[0]);
        verify(mock, times(1)).updateDate(1, new Date(0));
        verify(mock, times(1)).updateDate("field", new Date(0));
        verify(mock, times(1)).updateTime(1, new Time(0));
        verify(mock, times(1)).updateTime("field", new Time(0));
        verify(mock, times(1)).updateTimestamp(1, new Timestamp(0));
        verify(mock, times(1)).updateTimestamp("field", new Timestamp(0));

        verify(mock, times(1)).updateAsciiStream(eq(1), any(ByteArrayInputStream.class)); //new ByteArrayInputStream(new byte[0])
        verify(mock, times(1)).updateAsciiStream(eq("field"), any(ByteArrayInputStream.class));
        verify(mock, times(1)).updateBinaryStream(eq(1), any(ByteArrayInputStream.class));
        verify(mock, times(1)).updateBinaryStream(eq("field"), any(ByteArrayInputStream.class));
        verify(mock, times(1)).updateBinaryStream(eq(1), any(ByteArrayInputStream.class));
        verify(mock, times(1)).updateBinaryStream(eq("field"), any(ByteArrayInputStream.class));

        verify(mock, times(1)).updateObject(eq(1), any(Object.class));
        verify(mock, times(1)).updateObject(eq("field"), any(Object.class));
        verify(mock, times(1)).updateObject(eq(1), any(Object.class), eq(10));
        verify(mock, times(1)).updateObject(eq("field"), any(Object.class), eq(10));
        // These methods are implemented in ResultSet as and throw SQLFeatureNotSupportedException
        // TODO: implement these methods and uncomment tests
//        verify(mock, times(1)).updateObject(eq(1), any(Object.class), eq(JDBCType.BIGINT));
//        verify(mock, times(1)).updateObject(eq("field"), any(Object.class), eq(JDBCType.BIGINT));
//        verify(mock, times(1)).updateObject(eq(1), any(Object.class), eq(JDBCType.BIGINT), eq(10));
//        verify(mock, times(1)).updateObject(eq("field"), any(Object.class), eq(JDBCType.BIGINT), eq(10));

        verify(mock, times(1)).insertRow();
        verify(mock, times(1)).updateRow();
        verify(mock, times(1)).deleteRow();
        verify(mock, times(1)).refreshRow();
        verify(mock, times(1)).cancelRowUpdates();
        verify(mock, times(1)).moveToInsertRow();
        verify(mock, times(1)).moveToCurrentRow();
        verify(mock, times(1)).rowInserted();
        verify(mock, times(1)).rowUpdated();
        verify(mock, times(1)).rowDeleted();

        verify(mock, times(1)).updateRef(1, null);
        verify(mock, times(1)).updateRef("field", null);
        verify(mock, times(1)).updateBlob(eq(1), any(SerialBlob.class));
        verify(mock, times(1)).updateBlob(eq("field"), any(SerialBlob.class));
        verify(mock, times(1)).updateBlob(eq(1), any(ByteArrayInputStream.class));
        verify(mock, times(1)).updateBlob(eq("field"), any(ByteArrayInputStream.class));
        verify(mock, times(1)).updateBlob(eq(1), any(ByteArrayInputStream.class), eq(0L));
        verify(mock, times(1)).updateBlob(eq("field"), any(ByteArrayInputStream.class), eq(0L));
        verify(mock, times(1)).updateClob(eq(1), any(SerialClob.class));
        verify(mock, times(1)).updateClob(eq("field"), any(SerialClob.class));
        verify(mock, times(1)).updateClob(eq(1), any(StringReader.class));
        verify(mock, times(1)).updateClob(eq("field"), any(StringReader.class));
        verify(mock, times(1)).updateClob(eq(1), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateClob(eq("field"), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateArray(1, null);
        verify(mock, times(1)).updateArray("field", null);
        verify(mock, times(1)).updateRowId(1, null);
        verify(mock, times(1)).updateRowId("field", null);
        verify(mock, times(1)).updateNString(1, null);
        verify(mock, times(1)).updateNString("field", null);
        verify(mock, times(1)).updateNClob(1, (NClob)null);
        verify(mock, times(1)).updateNClob("field", (NClob)null);
        verify(mock, times(1)).updateNClob(1, (Reader)null);
        verify(mock, times(1)).updateNClob("field", (Reader)null);
        verify(mock, times(1)).updateNClob(1, (Reader)null, 0);
        verify(mock, times(1)).updateNClob("field", (Reader)null, 0);
        verify(mock, times(1)).updateSQLXML(1, null);
        verify(mock, times(1)).updateSQLXML("field", null);
        verify(mock, times(1)).updateNCharacterStream(1, null);
        verify(mock, times(1)).updateNCharacterStream("field", null);
        verify(mock, times(1)).updateAsciiStream(1, null);
        verify(mock, times(1)).updateAsciiStream("field", null);
        verify(mock, times(1)).updateAsciiStream(1, null);
        verify(mock, times(1)).updateAsciiStream("field", null);
        verify(mock, times(1)).updateAsciiStream(eq(1), any(ByteArrayInputStream.class), eq(0));
        verify(mock, times(1)).updateAsciiStream(eq("field"), any(ByteArrayInputStream.class), eq(0));
        verify(mock, times(1)).updateAsciiStream(eq(1), any(ByteArrayInputStream.class), eq(0L));
        verify(mock, times(1)).updateAsciiStream(eq("field"), any(ByteArrayInputStream.class), eq(0L));
        verify(mock, times(1)).updateBinaryStream(1, null);
        verify(mock, times(1)).updateBinaryStream("field", null);
        verify(mock, times(1)).updateBinaryStream(eq(1), any(ByteArrayInputStream.class), eq(0));
        verify(mock, times(1)).updateBinaryStream(eq("field"), any(ByteArrayInputStream.class), eq(0));
        verify(mock, times(1)).updateBinaryStream(eq(1), any(ByteArrayInputStream.class), eq(0L));
        verify(mock, times(1)).updateBinaryStream(eq("field"), any(ByteArrayInputStream.class), eq(0L));
        verify(mock, times(1)).updateCharacterStream(1, null);
        verify(mock, times(1)).updateCharacterStream("field", null);
        verify(mock, times(1)).updateCharacterStream(eq(1), any(StringReader.class), eq(0));
        verify(mock, times(1)).updateCharacterStream(eq("field"), any(StringReader.class), eq(0));
        verify(mock, times(1)).updateCharacterStream(eq(1), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateCharacterStream(eq("field"), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateNCharacterStream(1, null);
        verify(mock, times(1)).updateNCharacterStream("field", null);
        verify(mock, times(1)).updateNCharacterStream(eq(1), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateNCharacterStream(eq("field"), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateNCharacterStream(eq(1), any(StringReader.class), eq(0L));
        verify(mock, times(1)).updateNCharacterStream(eq("field"), any(StringReader.class), eq(0L));

    }


    private void assertUpdateValue(ResultSet rs) {
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
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateTimestamp(1, new Timestamp(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateTimestamp("field", new Timestamp(0)));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateAsciiStream("field", new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream(1, new ByteArrayInputStream(new byte[0])));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateBinaryStream("field", new ByteArrayInputStream(new byte[0])));
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
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob(1, (Reader)null, 0));
        assertThrows(SQLFeatureNotSupportedException.class, () -> rs.updateNClob("field", (Reader)null, 0));
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
    }
}