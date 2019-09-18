package com.nosqldriver.sql;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.util.Objects;

import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.String.format;

public interface DelegatingResultSet extends ResultSet {
    @Override
    default String getString(int columnIndex) throws SQLException {
        return getObject(columnIndex, String.class);
    }

    @Override
    default boolean getBoolean(int columnIndex) throws SQLException {
        return getObject(columnIndex, boolean.class);
    }

    @Override
    default byte getByte(int columnIndex) throws SQLException {
        return getObject(columnIndex, byte.class);
    }

    @Override
    default short getShort(int columnIndex) throws SQLException {
        return getObject(columnIndex, short.class);
    }

    @Override
    default int getInt(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), int.class);
    }

    @Override
    default long getLong(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), long.class);
    }

    @Override
    default float getFloat(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), float.class);
    }

    @Override
    default double getDouble(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), double.class);
    }

    @Override
    @Deprecated
    default BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("This method is deprecated. Use getBigDecimal(int columnIndex) instead.");
    }

    @Override
    default byte[] getBytes(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), byte[].class);
    }

    @Override
    default Date getDate(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Date.class);
    }

    @Override
    default Time getTime(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Time.class);
    }

    @Override
    default Timestamp getTimestamp(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Timestamp.class);
    }

    @Override
    default InputStream getAsciiStream(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), InputStream.class);
    }

    @Override
    @Deprecated
    default InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("This method is deprecated. Use getCharacterStream(int columnIndex) instead.");
    }

    @Override
    default InputStream getBinaryStream(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), InputStream.class);
    }

    @Override
    default String getString(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), String.class);
    }

    @Override
    default boolean getBoolean(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), boolean.class);
    }

    @Override
    default byte getByte(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), byte.class);
    }

    @Override
    default short getShort(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), short.class);
    }

    @Override
    default int getInt(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), int.class);
    }

    @Override
    default long getLong(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), long.class);
    }

    @Override
    default float getFloat(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), float.class);
    }

    @Override
    default double getDouble(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), double.class);
    }

    @Override
    @Deprecated
    default BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("This method is deprecated. Use getBigDecimal(String columnLabel) instead.");
    }

    @Override
    default byte[] getBytes(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), byte[].class);
    }

    @Override
    default Date getDate(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Date.class);
    }

    @Override
    default Time getTime(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Time.class);
    }

    @Override
    default Timestamp getTimestamp(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Timestamp.class);
    }

    @Override
    default InputStream getAsciiStream(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), InputStream.class);
    }

    @Override
    @Deprecated
    default InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("This method is deprecated. Use getCharacterStream(String columnLabel) instead.");
    }

    @Override
    default InputStream getBinaryStream(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), InputStream.class);
    }


    @Override
    default int findColumn(String columnLabel) throws SQLException {
        ResultSetMetaData md = getMetaData();
        int n = md.getColumnCount();
        for (int i = 0; i < n; i++) {
            if (Objects.equals(columnLabel, md.getColumnLabel(i))) {
                return i;
            }
        }
        throw new SQLException(format("Column %s is not found", columnLabel));
    }

    @Override
    default Reader getCharacterStream(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Reader.class);
    }

    @Override
    default Reader getCharacterStream(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Reader.class);
    }

    @Override
    default BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), BigDecimal.class);
    }

    @Override
    default BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), BigDecimal.class);
    }

    @Override
    default Blob getBlob(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Blob.class);
    }

    @Override
    default Clob getClob(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Clob.class);
    }

    @Override
    default Array getArray(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), Array.class);
    }

    @Override
    default Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Blob getBlob(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Blob.class);
    }

    @Override
    default Clob getClob(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Clob.class);
    }

    @Override
    default Array getArray(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), Array.class);
    }

    @Override
    default Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default URL getURL(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), URL.class);
    }

    @Override
    default URL getURL(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), URL.class);
    }

    @Override
    default NClob getNClob(int columnIndex) throws SQLException {
        return cast(getObject(columnIndex), NClob.class);
    }

    @Override
    default NClob getNClob(String columnLabel) throws SQLException {
        return cast(getObject(columnLabel), NClob.class);
    }

    @Override
    default SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    default SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    default String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    default String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    default Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    default Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(columnLabel);
    }

    @Override
    default <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return cast(getObject(columnIndex), type);
    }

    @Override
    default <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return cast(getObject(columnLabel), type);
    }

    @Override
    default Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

}
