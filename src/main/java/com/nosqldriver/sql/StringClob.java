package com.nosqldriver.sql;

import com.nosqldriver.util.SneakyThrower;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.util.Objects;

import static com.nosqldriver.util.SneakyThrower.sneakyThrow;
import static java.lang.String.format;

public class StringClob implements NClob {
    private String data;

    public StringClob() {
        this("");
    }

    public StringClob(String data) {
        this.data = data;
    }


    @Override
    public long length() throws SQLException {
        return data.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        int from = (int)pos - 1;
        return data.substring(from, from + length);
    }

    @Override
    public Reader getCharacterStream() throws SQLException {
        return new StringReader(data);
    }

    @Override
    public InputStream getAsciiStream() throws SQLException {
        return new ByteArrayInputStream(data.getBytes());
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        if (start > Integer.MAX_VALUE || start < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, start));
        }
        int from = (int)start - 1;
        int foundIndex = data.indexOf(searchstr, from);
        return foundIndex < 0 ? foundIndex : foundIndex + 1;
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        return position(((StringClob)searchstr).data, start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return setString(pos, str, 0, str.length());
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        if (offset < 0) {
            throw new SQLException(format("Offset cannot be negative but was %d", offset));
        }
        int till = (int)pos;
        data = (data.length() >= till ? data.substring(0, till) : data) + str.substring(offset, offset + len);
        return len - offset;
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        if (pos > Integer.MAX_VALUE || pos < 1) {
            throw new SQLException(format("Position must be between 1 and %d but was %d", Integer.MAX_VALUE, pos));
        }
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                super.close();
                SneakyThrower.sqlCall(() -> setString(pos, new String(toByteArray())));
            }
        };
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {

        return new StringWriter() {
            @Override
            public void close() throws IOException {
                super.close();
                SneakyThrower.sqlCall(() -> setString(pos, getBuffer().toString()));
            }
        };
    }

    @Override
    public void truncate(long len) throws SQLException {
        data = data.substring(0, (int)len);
    }

    @Override
    public void free() throws SQLException {
        data = "";
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(getSubString(pos, (int)length));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return Objects.equals(data, ((StringClob) o).data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data);
    }
}
