package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class ByteArrayComparatorTest {
    private final Comparator<byte[]> comparator = new ByteArrayComparator();

    @Test
    void nullVsNull() {
        assertEquals(0, comparator.compare(null, null));
    }

    @Test
    void emptyVsEmpty() {
        assertEquals(0, comparator.compare(new byte[0], new byte[0]));
    }

    @Test
    void nullVsEmpty() {
        assertEquals(-1, comparator.compare(null, new byte[0]));
    }

    @Test
    void emptyVsNull() {
        assertEquals(1, comparator.compare(new byte[0], null));
    }

    @Test
    void oneEqByte() {
        assertEquals(0, comparator.compare(new byte[] {'a'}, new byte[] {'a'}));
    }

    @Test
    void oneByteNegative() {
        assertTrue(comparator.compare(new byte[] {'a'}, new byte[] {'b'}) < 0);
    }

    @Test
    void oneBytePositive() {
        assertTrue(comparator.compare(new byte[] {'b'}, new byte[] {'a'}) > 0);
    }

    @Test
    void severalEqualBytes() {
        assertEquals(0, comparator.compare("hello".getBytes(), "hello".getBytes()));
    }

    @Test
    void severalBytesNegative() {
        assertTrue(comparator.compare("hello".getBytes(), "world".getBytes()) < 0);
    }

    @Test
    void severalBytesDifferentLengthNegative() {
        assertTrue(comparator.compare("hello".getBytes(), "hello world".getBytes()) < 0);
    }

    @Test
    void severalBytesDifferentLengthPositive() {
        assertTrue(comparator.compare("hello world".getBytes(), "hello".getBytes()) > 0);
    }
}