package com.optimization.datastructure;

import java.util.Arrays;

public enum One {
    ONE("one"), TWO("two"), THREE("three"),
    ;
    private final String label;


    One(String label) {
        this.label = label;
    }

    public static One forLabel(String label) {
        return Arrays.stream(values()).filter(e -> e.label.equals(label)).findFirst().orElseThrow(() -> new IllegalArgumentException(label));
    }
}
