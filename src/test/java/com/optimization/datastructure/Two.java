package com.optimization.datastructure;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum Two {
    ONE("one"), TWO("two"), THREE("three"),
    ;
    private final String label;
    private final static Map<String, Two> map = Arrays.stream(values()).collect(Collectors.toMap(e -> e.label, e -> e));


    Two(String label) {
        this.label = label;
    }

    public static Two forLabel(String label) {
        return Optional.ofNullable(map.get(label)).orElseThrow(() -> new IllegalArgumentException(label));
    }
}
