package com.nosqldriver.sql;

import com.nosqldriver.util.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StringClobTest {
    private static final String HELLO = "hello";
    private static final String WORLD = "world";
    private static final String HELLO_WORLD = HELLO + " " + WORLD;

    @Test
    void empty() throws SQLException {
        assertEquals(0, new StringClob().length());
    }

    @Test
    void setEmptyString() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, "");
        assertEquals(0, clob.length());
    }

    @Test
    void setString() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO);
        assertEquals(HELLO.length(), clob.length());
        String s = clob.getSubString(1, HELLO.length());
        assertEquals(HELLO, s);
    }

    @Test
    void appendString() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO);
        clob.setString(6, " ");
        clob.setString(7, WORLD);
        assertEquals(HELLO_WORLD.length(), clob.length());
        String s = clob.getSubString(1, HELLO_WORLD.length());
        assertEquals(HELLO_WORLD, s);
    }

    @Test
    void replaceString() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        String s = clob.getSubString(1, HELLO_WORLD.length());
        assertEquals(HELLO_WORLD, s);
        clob.setString(6, "nclob");
        s = clob.getSubString(1, HELLO_WORLD.length());
        assertEquals("hello nclob", s);
    }

    @Test
    void free() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO);
        assertEquals(HELLO.length(), clob.length());
        String s = clob.getSubString(1, HELLO.length());
        assertEquals(HELLO, s);
        clob.free();
        assertEquals(0, clob.length());
    }

    @Test
    void setCharacterStream() throws SQLException, IOException {
        Clob clob = new StringClob();
        Writer w = clob.setCharacterStream(1);
        w.write(HELLO_WORLD);
        w.close();
        String s = clob.getSubString(1, HELLO_WORLD.length());
        assertEquals(HELLO_WORLD, s);
    }

    @Test
    void setAsciiStream() throws SQLException, IOException {
        Clob clob = new StringClob();
        OutputStream os = clob.setAsciiStream(1);
        os.write(HELLO_WORLD.getBytes());
        os.close();
        String s = clob.getSubString(1, HELLO_WORLD.length());
        assertEquals(HELLO_WORLD, s);
    }

    @Test
    void getCharacterStream() throws SQLException, IOException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        assertEquals(HELLO_WORLD, IOUtils.toString(clob.getCharacterStream()));
    }

    @Test
    void truncate() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        clob.truncate(5);
        String s = clob.getSubString(1, HELLO.length());
        assertEquals(HELLO, s);
    }
}