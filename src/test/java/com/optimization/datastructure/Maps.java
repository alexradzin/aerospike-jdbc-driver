package com.optimization.datastructure;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.System.nanoTime;

class Maps {
    private static final int VALUES_COUNT = 1_000_000;
    private static final int TEST_COUNT = 1_000_000;
    private static final int TO_MS = 1_000_000;


    @Test
    void intHashMap() {
        testIntKey("HashMap<Integer, Boolean>", new HashMap<>());
    }

    @Test
    void intTreeMap() {
        testIntKey("TreeMap<Integer, Boolean>", new TreeMap<>());
    }

    @Test
    void stringHashMap() {
        testStringKey("HashMap<String, Boolean>", new HashMap<>());
    }

    @Test
    void stringTreeMap() {
        testStringKey("TreeMap<String, Boolean>", new TreeMap<>());
    }

    @Test
    void stringTreeMapCaseInsensitive() {
        testStringKey("TreeMap<String, Boolean>(CASE_INSENSITIVE_ORDER)", new TreeMap<>(CASE_INSENSITIVE_ORDER));
    }

    @Test
    void capitalizedStringHashMap() {
        testCapitalizedStringKey("HashMap<String, Boolean>, capitalized", new HashMap<>());
    }


    private void testIntKey(String testName, Map<Integer, Boolean> map) {
        long beforePut = nanoTime();
        for (int i = 0; i < VALUES_COUNT; i++) {
            map.put(i, Boolean.TRUE);
        }
        long afterPut = nanoTime();

        int notFalseCount = 0;
        for (int i = 0; i < VALUES_COUNT; i++) {
            notFalseCount += map.get(i).compareTo(Boolean.FALSE);
        }
        long afterGet = nanoTime();
        System.out.printf("%s: put: %d, get: %d, notFalseCount: %d\n", testName, (afterPut - beforePut) / TO_MS, (afterGet - afterPut) / TO_MS, notFalseCount);
    }

    private void testStringKey(String testName, Map<String, Boolean> map) {
        String[] keys = new String[VALUES_COUNT];
        for (int i = 0; i < VALUES_COUNT; i++) {
            keys[i] = "" + i;
        }

        long beforePut = nanoTime();
        for (int i = 0; i < VALUES_COUNT; i++) {
            map.put(keys[i], Boolean.TRUE);
        }
        long afterPut = nanoTime();

        int notFalseCount = 0;
        for (int i = 0; i < VALUES_COUNT; i++) {
            notFalseCount += map.get(keys[i]).compareTo(Boolean.FALSE);
        }
        long afterGet = nanoTime();
        System.out.printf("%s: put: %d, get: %d, notFalseCount: %d\n", testName, (afterPut - beforePut) / TO_MS, (afterGet - afterPut) / TO_MS, notFalseCount);
    }

    private void testCapitalizedStringKey(String testName, Map<String, Boolean> map) {
        String[] keys = new String[VALUES_COUNT];
        for (int i = 0; i < VALUES_COUNT; i++) {
            keys[i] = "" + i;
        }

        long beforePut = nanoTime();
        for (int i = 0; i < VALUES_COUNT; i++) {
            map.put(keys[i].toUpperCase(), Boolean.TRUE);
        }
        long afterPut = nanoTime();

        int notFalseCount = 0;
        for (int i = 0; i < VALUES_COUNT; i++) {
            notFalseCount += map.get(keys[i].toUpperCase()).compareTo(Boolean.FALSE);
        }
        long afterGet = nanoTime();
        System.out.printf("%s: put: %d, get: %d, notFalseCount: %d\n", testName, (afterPut - beforePut) / TO_MS, (afterGet - afterPut) / TO_MS, notFalseCount);
    }
}
