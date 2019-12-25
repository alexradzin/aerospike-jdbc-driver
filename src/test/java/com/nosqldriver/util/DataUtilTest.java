package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataUtilTest {
    @Test
    void emptyArray() {
        assertEquals(0, DataUtil.toArray(Collections.emptyMap()).length);
    }

    @Test
    void singletonArray() {
        assertArrayEquals(new Object[] {"zero"}, DataUtil.toArray(Collections.singletonMap("0",  "zero")));
    }

    @Test
    void normalArray() {
        Map<String, Object> data = new HashMap<>();
        data.put("0", "zero");
        data.put("1", "one");
        data.put("2", "two");
        assertArrayEquals(new Object[] {"zero", "one", "two"}, DataUtil.toArray(data));
    }

    @Test
    void arrayWithMissingEntries() {
        Map<String, Object> data = new HashMap<>();
        data.put("0", "zero");
        data.put("2", "two");
        assertEquals("Cannot create list due to missing entries", assertThrows(IllegalArgumentException.class, () -> DataUtil.toArray(data)).getMessage());
    }

}