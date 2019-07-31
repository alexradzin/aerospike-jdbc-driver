package com.nosqldriver.util;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {
    @Override
    public int compare(byte[] o1, byte[] o2) {
        if (o2 == null && o1 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
        // both not null
        int n = Math.min(o1.length, o2.length);
        for (int i = 0; i < n; i++) {
            int c = o1[i] - o2[i];
            if (c != 0) {
                return c;
            }
        }
        return o1.length - o2.length;
    }
}
