package com.optimization.datastructure;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class LabeledEnumTest {
    @Test
    void labelExists1() {
        assertEquals(One.ONE, One.forLabel("one"));
    }

    @Test
    void labelExists2() {
        assertEquals(Two.TWO, Two.forLabel("two"));
    }



    @Test
    void bechmark() {
        for (int i = 0; i < 10; i++) {
            bechmark1();
            bechmark2();
        }
    }


    void bechmark1() {
        long before = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            One.forLabel("three");
        }
        long after = System.currentTimeMillis();
        System.out.println("Search: " + (after - before) + " ms");
    }

    void bechmark2() {
        long before = System.currentTimeMillis();
        for (int i = 0; i < 10_000_000; i++) {
            Two.forLabel("three");
        }
        long after = System.currentTimeMillis();
        System.out.println("Map: " + (after - before) + " ms");
    }
}