package com.nosqldriver.sql;

import com.nosqldriver.util.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    void getSubCharacterStream() throws SQLException, IOException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        assertEquals(HELLO_WORLD, IOUtils.toString(clob.getCharacterStream(1, HELLO_WORLD.length())));
        assertEquals(HELLO, IOUtils.toString(clob.getCharacterStream(1, HELLO.length())));
        assertEquals(WORLD, IOUtils.toString(clob.getCharacterStream(7, WORLD.length())));
    }

    @Test
    void truncate() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        clob.truncate(5);
        String s = clob.getSubString(1, HELLO.length());
        assertEquals(HELLO, s);
    }

    @Test
    void getSubStringWrongPos() throws SQLException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        assertThrows(SQLException.class, () -> clob.getSubString(0, 1));
    }

    @Test
    void position() throws SQLException {
        Clob clobHelloWorld = new StringClob();
        clobHelloWorld.setString(1, HELLO_WORLD);
        assertEquals(1, clobHelloWorld.position(HELLO_WORLD, 1));
        assertEquals(1, clobHelloWorld.position(HELLO, 1));
        assertEquals(7, clobHelloWorld.position(WORLD, 1));
        assertEquals(-1, clobHelloWorld.position("does not exist", 1));

        Clob clobHello = new StringClob();
        clobHello.setString(1, HELLO);
        assertEquals(1, clobHelloWorld.position(clobHelloWorld, 1));
        assertEquals(1, clobHelloWorld.position(clobHello, 1));

        assertThrows(SQLException.class, () -> clobHelloWorld.position(WORLD, -1));
        assertThrows(SQLException.class, () -> clobHelloWorld.position(WORLD, Integer.MAX_VALUE + 1L));
    }

    @Test
    void getAsciiStream() throws SQLException, IOException {
        Clob clob = new StringClob();
        clob.setString(1, HELLO_WORLD);
        assertEquals(HELLO_WORLD, new String(IOUtils.toByteArray(clob.getAsciiStream())));
    }

    @Test
    void setStringWrongEdges() {
        Clob clob = new StringClob();
        assertThrows(SQLException.class, () -> clob.setString(-1, ""));
        assertThrows(SQLException.class, () -> clob.setString(Integer.MAX_VALUE + 1L, ""));
        assertThrows(SQLException.class, () -> clob.setString(1, "", -1, 0));
    }
}