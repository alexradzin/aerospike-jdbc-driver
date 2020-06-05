package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Collections.singletonMap;
import static java.util.Comparator.comparingInt;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataUtilTest {
    private final DataUtil dataUtil = new DataUtil();
    @Test
    void emptyArray() {
        assertEquals(0, dataUtil.toArray(Collections.emptyMap()).length);
    }

    @Test
    void singletonArray() {
        assertArrayEquals(new Object[] {"zero"}, dataUtil.toArray(singletonMap("0",  "zero")));
    }

    @Test
    void normalArray() {
        Map<String, Object> data = new HashMap<>();
        data.put("0", "zero");
        data.put("1", "one");
        data.put("2", "two");
        assertArrayEquals(new Object[] {"zero", "one", "two"}, dataUtil.toArray(data));
    }

    @Test
    void arrayWithMissingEntries() {
        Map<String, Object> data = new HashMap<>();
        data.put("0", "zero");
        data.put("2", "two");
        assertEquals("Cannot create list due to missing entries", assertThrows(SQLException.class, () -> dataUtil.toArray(data)).getMessage());
    }

    @Test
    void arrayWithDuplicateEntries() {
        // A way to create map with duplicate entries:
        // 1. create TreeMap with comparator based on identity hashCode
        Map<String, Object> data = new TreeMap<>(comparingInt(System::identityHashCode));
        // 2. create keys by explicity calling of the String constructor.
        data.put(new String("0"), "something");
        data.put(new String("0"), "something");
        assertEquals("Cannot create list due to duplicate entries", assertThrows(SQLException.class, () -> dataUtil.toArray(data)).getMessage());
    }
}