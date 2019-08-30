package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.text.ParseException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}