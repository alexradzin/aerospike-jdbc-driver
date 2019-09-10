package com.optimization.datastructure;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;
import java.util.regex.Pattern;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;

public class Strings {
    private static final int TEST_COUNT = 1_000_000;
    private static final int TO_MS = 1_000_000;
    private static final String[] phones = new String[TEST_COUNT];
    static {
        Random rnd = new Random(currentTimeMillis());
        for (int i = 0; i < TEST_COUNT; i++) {
            String phone = "";
            for (int d = 0; d < 9; d++) {
                phone += "" + rnd.nextInt();
            }
            phones[i] = phone;
        }
    }


    @Test
    void noise() {
        long before = nanoTime();
        long before2 = nanoTime();
        int count = 0;
        for (String phone : phones) {
            if (before2 >= before) {
                count++;
            }
        }
        long after = nanoTime();
        System.out.printf("noise: time: %d, count: %d\n", (after - before) / TO_MS, count);
    }


    @Test
    void startsWith() {
        long before = nanoTime();
        int count = 0;
        for (String phone : phones) {
            if (phone.startsWith("1")) {
                count++;
            }
        }
        long after = nanoTime();
        System.out.printf("startsWith: time: %d, count: %d\n", (after - before) / TO_MS, count);
    }

    @Test
    void startsWithRegex() {
        long before = nanoTime();
        int count = 0;
        for (String phone : phones) {
            if (Pattern.compile("^1").matcher(phone).find()) {
                count++;
            }
        }
        long after = nanoTime();
        System.out.printf("startsWithRegex: time: %d, count: %d\n", (after - before) / TO_MS, count);
    }

    @Test
    void startsPrecompiledWithRegex() {
        Pattern regex = Pattern.compile("^1");
        long before = nanoTime();
        int count = 0;
        for (String phone : phones) {
            if (regex.matcher(phone).find()) {
                count++;
            }
        }
        long after = nanoTime();
        System.out.printf("startsPrecompiledWithRegex: time: %d, count: %d\n", (after - before) / TO_MS, count);
    }
}
