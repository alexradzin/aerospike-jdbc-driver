package com.nosqldriver.util;

import com.nosqldriver.Person;
import com.nosqldriver.aerospike.sql.TestDataUtils;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PartialComparatorTest {
    @Test
    void selfEq() {
        assertEquals(0, new PartialComparator<String, String>(s -> s, CASE_INSENSITIVE_ORDER).compare("aBc", "AbC"));
    }

    @Test
    void selfLt() {
        assertEquals(-1, new PartialComparator<Integer, Integer>(s -> s, (i, j) -> Double.valueOf(Math.signum(i - j)).intValue()).compare(5, 8));
    }


    @Test
    void stringLengthGt() {
        assertEquals(1, new PartialComparator<>(String::length, (i, j) -> Double.valueOf(Math.signum(i - j)).intValue()).compare("greetings", "world"));
    }


    @Test
    void mapValueComparator() {
        Comparator<Map<String, Object>> comparator = new PartialComparator<>(m -> (Integer)m.get("year_of_birth"), (y1, y2) -> Double.valueOf(Math.signum(y1 - y2)).intValue());

        Function<Person, Map<String, Object>> toMap = person -> {
            Map<String, Object> map = new HashMap<>();
            map.put("name", person.getFirstName() + " " + person.getLastName());
            map.put("year_of_birth", person.getYearOfBirth());
            map.put("kids_count", person.getYearOfBirth());
            return map;
        };

        List<Map<String, Object>> beatlsAttrs = Arrays.stream(TestDataUtils.beatles).map(toMap).sorted(comparator).collect(toList());
        assertArrayEquals(new String[] {"John Lennon", "Ringo Starr", "Paul McCartney", "George Harrison"}, beatlsAttrs.stream().map(p -> p.get("name")).toArray());
    }

}