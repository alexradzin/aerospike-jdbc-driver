package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.text.ParseException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

class SneakyThrowerTest {
    @Test
    void throwRuntimeException() {
        RuntimeException e = assertThrows(RuntimeException.class, () -> SneakyThrower.sneakyThrow(new RuntimeException("rt")));
        assertEquals("rt", e.getMessage());
    }

    @Test
    void throwCheckedException() {
        ParseException e = assertThrows(ParseException.class, () -> SneakyThrower.sneakyThrow(new ParseException("parse", 123)));
        assertEquals("parse", e.getMessage());
        assertEquals(123, e.getErrorOffset());
    }

    @Test
    void getNormally() {
        assertEquals(123, Objects.requireNonNull(SneakyThrower.get(() -> 123)).intValue());
    }

    @Test
    void getWithException() {
        // assertThrows does not work for sneaky thrower...
        try {
            SneakyThrower.get(() -> {
                throw new SQLException("test");
            });
            fail();
        } catch (Exception e) {
            assertEquals(SQLException.class, e.getClass());
            assertEquals("test", e.getMessage());
        }
    }
}