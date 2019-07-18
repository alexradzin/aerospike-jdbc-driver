package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map.Entry;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CompositeComparatorTest {
    @Test
    void noComparators() {
        assertEquals(0, new CompositeComparator<>().compare("a", "b"));
    }

    @Test
    void oneComparator() {
        Comparator<String> comparator = new CompositeComparator<>(CASE_INSENSITIVE_ORDER);
        assertEquals(-1, comparator.compare("a", "b"));
        assertEquals(0, comparator.compare("a", "a"));
        assertEquals(1, comparator.compare("b", "a"));
    }


    @Test
    void twoComparators() {
        Comparator<String> comparator = new CompositeComparator<>(
                new PartialComparator<>(s -> s.split(",")[0], CASE_INSENSITIVE_ORDER),
                new PartialComparator<>(s -> Integer.parseInt(s.split(",")[1]), (i, j) -> Double.valueOf(Math.signum(i - j)).intValue())
        );

        // Alphabetically abd,10 < abc,2 because character 1 is less then character 2.
        // But we split the strings and compare them alphanumerically, so abc,10 > abc,2 because numerically 10> 2
        assertEquals(1, comparator.compare("abc,10", "abc,2"));
    }


    @Test
    void mapEntriesComparator() {
        Entry<String, ?> lennon = Collections.singletonMap("John", 1940).entrySet().iterator().next();
        Entry<String, ?> kennedy = Collections.singletonMap("John", 1917).entrySet().iterator().next();

        // Compares names and age taking map entry that has person name as a key and year of birth as a value.
        // Year of birth is input, while we compare ages here; this is why year  arguments swithch places in calculation: y2 - y1
        Comparator<Entry<String, ?>> comparator = new CompositeComparator<>(
                new PartialComparator<>(Entry::getKey, CASE_INSENSITIVE_ORDER),
                new PartialComparator<>(e -> (Integer)e.getValue(), (y1, y2) -> Double.valueOf(Math.signum(y2 - y1)).intValue())
        );

        assertEquals(-1, comparator.compare(lennon, kennedy));
        assertEquals(1, comparator.compare(kennedy, lennon));
    }


}