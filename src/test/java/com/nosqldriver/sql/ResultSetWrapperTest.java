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
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResultSetWrapperTest {
    @Test
    void wrapping() throws SQLException {
        ResultSet mock = mock(ResultSet.class);
        ResultSet rs = new ResultSetWrapper(mock, emptyList(), true);

        SQLWarning warning = new SQLWarning();
        when(mock.getWarnings()).thenReturn(warning);
        assertEquals(warning, rs.getWarnings());

        rs.clearWarnings();

        when(mock.getCursorName()).thenReturn("cursor1");
        assertEquals("cursor1", rs.getCursorName());

        when(mock.findColumn("column1")).thenReturn(5);
        assertEquals(5, rs.findColumn("column1"));

        when(mock.isBeforeFirst()).thenReturn(true, false, false, true);
        assertTrue(rs.isBeforeFirst());
        assertFalse(rs.isBeforeFirst());
        assertFalse(rs.isBeforeFirst());
        assertTrue(rs.isBeforeFirst());

        when(mock.isAfterLast()).thenReturn(false, false, true);
        assertFalse(rs.isAfterLast());
        assertFalse(rs.isAfterLast());
        assertTrue(rs.isAfterLast());

        when(mock.isFirst()).thenReturn(false, true, false);
        assertFalse(rs.isFirst());
        assertTrue(rs.isFirst());
        assertFalse(rs.isFirst());


        when(mock.isLast()).thenReturn(false, false, false, true);
        assertFalse(rs.isLast());
        assertFalse(rs.isLast());
        assertFalse(rs.isLast());
        assertTrue(rs.isLast());

        rs.beforeFirst();
        rs.afterLast();

        when(mock.last()).thenReturn(false, true);
        assertFalse(rs.last());
        assertTrue(rs.last());

        when(mock.first()).thenReturn(true, false);
        assertTrue(rs.first());
        assertFalse(rs.first());


        when(mock.absolute(123)).thenReturn(false);
        when(mock.absolute(321)).thenReturn(true);
        assertFalse(rs.absolute(123));
        assertTrue(rs.absolute(321));

        when(mock.relative(456)).thenReturn(true);
        when(mock.relative(789)).thenReturn(false);
        assertTrue(rs.relative(456));
        assertFalse(rs.relative(789));

        when(mock.previous()).thenReturn(false, true);
        assertFalse(rs.previous());
        assertTrue(rs.previous());

        rs.setFetchDirection(1);
        when(mock.getFetchDirection()).thenReturn(1);
        assertEquals(1, rs.getFetchDirection());

        when(mock.getFetchSize()).thenReturn(2);
        assertEquals(2, rs.getFetchSize());

        when(mock.getType()).thenReturn(3);
        assertEquals(3, rs.getType());

        when(mock.getConcurrency()).thenReturn(4);
        assertEquals(4, rs.getConcurrency());

        Statement statement = mock(Statement.class);
        when(mock.getStatement()).thenReturn(statement);
        assertEquals(statement, rs.getStatement());

        when(mock.unwrap(String.class)).thenReturn("hello");
        assertEquals("hello", rs.unwrap(String.class));

        when(mock.isWrapperFor(Integer.class)).thenReturn(true, false);
        assertTrue(rs.isWrapperFor(Integer.class));
        assertFalse(rs.isWrapperFor(Integer.class));


        when(mock.getRow()).thenReturn(0, 1, 2);
        assertEquals(0, rs.getRow());
        assertEquals(1, rs.getRow());
        assertEquals(2, rs.getRow());

        rs.setFetchSize(1024);

        when(mock.getHoldability()).thenReturn(5);
        assertEquals(5, rs.getHoldability());

        when(mock.isClosed()).thenReturn(false, true);
        assertFalse(rs.isClosed());
        assertTrue(rs.isClosed());

        verify(mock, times(1)).getWarnings();
        verify(mock, times(1)).clearWarnings();
        verify(mock, times(1)).getCursorName();
        verify(mock, times(1)).findColumn("column1");
        verify(mock, times(4)).isBeforeFirst();
        verify(mock, times(3)).isAfterLast();
        verify(mock, times(3)).isFirst();
        verify(mock, times(4)).isLast();
        verify(mock, times(1)).beforeFirst();
        verify(mock, times(1)).afterLast();
        verify(mock, times(2)).last();
        verify(mock, times(2)).first();
        verify(mock, times(1)).absolute(123);
        verify(mock, times(1)).absolute(321);
        verify(mock, times(1)).relative(456);
        verify(mock, times(1)).relative(789);
        verify(mock, times(2)).previous();
        verify(mock, times(1)).setFetchDirection(1);
        verify(mock, times(1)).getFetchDirection();
        verify(mock, times(1)).getFetchSize();
        verify(mock, times(1)).getType();
        verify(mock, times(1)).getConcurrency();
        verify(mock, times(1)).getStatement();
        verify(mock, times(1)).unwrap(String.class);
        verify(mock, times(2)).isWrapperFor(Integer.class);
        verify(mock, times(3)).getRow();
        verify(mock, times(1)).setFetchSize(1024);
        verify(mock, times(1)).getHoldability();
        verify(mock, times(2)).isClosed();
    }

    @Test
    void updateValueResultSetWrapperWithMock() throws SQLException {
        ResultSet mock = mock(ResultSet.class);
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
        rs.updateObject(1, new Object(), JDBCType.BIGINT);
        rs.updateObject("field", new Object(), JDBCType.BIGINT);
        rs.updateObject(1, new Object(), JDBCType.BIGINT, 10);
        rs.updateObject("field", new Object(), JDBCType.BIGINT, 10);

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
        verify(mock, times(1)).updateObject(eq(1), any(Object.class), eq(JDBCType.BIGINT));
        verify(mock, times(1)).updateObject(eq("field"), any(Object.class), eq(JDBCType.BIGINT));
        verify(mock, times(1)).updateObject(eq(1), any(Object.class), eq(JDBCType.BIGINT), eq(10));
        verify(mock, times(1)).updateObject(eq("field"), any(Object.class), eq(JDBCType.BIGINT), eq(10));

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

}