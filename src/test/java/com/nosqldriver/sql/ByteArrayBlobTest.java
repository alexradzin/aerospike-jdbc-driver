package com.nosqldriver.sql;

import com.nosqldriver.util.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ByteArrayBlobTest {
    @Test
    void empty() throws SQLException {
        assertArrayEquals(new byte[0], getBytes(new ByteArrayBlob()));
    }

    @Test
    void setEmpty() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, new byte[0]);
        assertArrayEquals(new byte[0], getBytes(blob));
    }

    @Test
    void setOneByte() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, new byte[] {123});
        assertArrayEquals(new byte[] {123}, getBytes(blob));
    }

    @Test
    void setSeveralBytes() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello".getBytes());
        assertArrayEquals("hello".getBytes(), getBytes(blob));
    }

    @Test
    void setSeveralBytesSeveralTimes() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello".getBytes());
        blob.setBytes(6, " ".getBytes());
        blob.setBytes(7, "world".getBytes());
        assertEquals("hello world", new String(getBytes(blob)));
    }

    @Test
    void setSeveralBytesReplace() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        blob.setBytes(7, "blob!".getBytes());
        assertEquals("hello blob!", new String(getBytes(blob)));
    }

    @Test
    void setStream() throws SQLException, IOException {
        Blob blob = new ByteArrayBlob();
        OutputStream os = blob.setBinaryStream(1);
        os.write(1);
        os.flush();
        os.close();
        assertEquals(1, blob.length());
        assertArrayEquals(new byte[] {1}, getBytes(blob));
    }


    @Test
    void positionExists() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        assertEquals(7, blob.position("world".getBytes(), 1));
    }
    @Test
    void positionBlobExists() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        Blob pattern = new ByteArrayBlob();
        pattern.setBytes(1, "world".getBytes());
        assertEquals(7, blob.position(pattern, 1));
    }


    @Test
    void positionFirst() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        assertEquals(1, blob.position("hello".getBytes(), 1));
    }

    @Test
    void positionDoesNotExist() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        assertEquals(-1, blob.position("bye".getBytes(), 1));
    }


    @Test
    void truncate() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        blob.truncate(5);
        assertArrayEquals("hello".getBytes(), getBytes(blob));
    }

    @Test
    void truncateZero() throws SQLException {
        assertThrows(SQLException.class, () -> new ByteArrayBlob().truncate(0));
    }

    @Test
    void free() throws SQLException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        assertEquals(11, blob.length());
        blob.free();
        assertEquals(0, blob.length());
    }

    @Test
    void geStream() throws SQLException, IOException {
        Blob blob = new ByteArrayBlob();
        blob.setBytes(1, "hello world".getBytes());
        assertArrayEquals("hello world".getBytes(), IOUtils.toByteArray(blob.getBinaryStream()));
    }



    private byte[] getBytes(Blob blob) throws SQLException {
        return blob.getBytes(1, (int)blob.length());
    }

}