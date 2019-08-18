package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.TestDataUtils;
import com.nosqldriver.sql.OrderItem.Direction;
import com.nosqldriver.util.PojoHelper;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.OrderItem.Direction.ASC;
import static com.nosqldriver.sql.OrderItem.Direction.DESC;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OrderItemsComparatorTest {
    private static List<Map<String, Object>> peopleMap = Arrays.stream(TestDataUtils.beatles).map(PojoHelper::fieldsToMap).collect(Collectors.toList());

    @Test
    void intEq() {
        assertIntValue(ASC, 123, 123, 0);
        assertIntValue(DESC, 456, 456, 0);
    }

    @Test
    void intGt() {
        assertIntValue(ASC, 456, 123, 1);
        assertIntValue(DESC, 456, 123, -1);
    }

    @Test
    void intLt() {
        assertIntValue(ASC, 123, 456, -1);
        assertIntValue(DESC, 123, 456, 1);

        assertAnyValue(ASC, 123, null, 1);
        assertAnyValue(DESC, 0, null, -1);
    }

    @Test
    void shortLt() {
        assertIntValue(ASC, (short)123, (short)456, -1);
        assertIntValue(DESC, (short)123, (short)456, 1);
    }

    @Test
    void byteLt() {
        assertIntValue(ASC, (byte)12, (byte)34, -1);
        assertIntValue(DESC, (byte)12, (byte)34, 1);
        assertAnyValue(DESC, null, (byte)1, 1);
    }


    @Test
    void numbersOfDifferentTypes() {
        assertAnyValue(ASC, 123L, 456, -1);
        assertAnyValue(DESC, 123, 456L, 1);
        assertAnyValue(DESC, (short)123, 123L, 0);
    }

    @Test
    void floatingPointNumbersOfDifferentTypes() {
        assertAnyValue(ASC, 3.14f, 2.1718281828, 1);
        assertAnyValue(DESC, 3.14, 2.1718281828f, -1);
        assertAnyValue(ASC, 3.14f, 3, 1);
        assertAnyValue(ASC, 3.14, 4, -1);
        assertAnyValue(DESC, 3, 2.7, -1);
        assertAnyValue(ASC, 4, 3.14, 1);
    }

    @Test
    void booleans() {
        assertAnyValue(ASC, false, true, -1);
        assertAnyValue(ASC, true, true, 0);
        assertAnyValue(ASC, true, false, 1);

        assertAnyValue(DESC, false, true, 1);
        assertAnyValue(DESC, true, true, 0);
        assertAnyValue(DESC, true, false, -1);

        assertAnyValue(DESC, true, "true", 0);
        assertAnyValue(ASC, true, "false", 1);
        assertAnyValue(DESC, "true", "true", 0);
        assertAnyValue(ASC, "true", true, 0);
    }


    @Test
    void strings() {
        assertAnyValue(ASC, "", "", 0);
        assertAnyValue(ASC, "x", "x", 0);
        assertAnyValue(ASC, "a", "b", -1);
        assertAnyValue(DESC, "a", "b", 1);
        assertAnyValue(ASC, "abc", "ab", 1);
        assertAnyValue(ASC, "abc", "abd", -1);
    }

    @Test
    void bytes() {
        assertAnyValue(ASC, "".getBytes(), "".getBytes(), 0);
        assertAnyValue(ASC, "x".getBytes(), "x".getBytes(), 0);
        assertAnyValue(ASC, "a".getBytes(), "b".getBytes(), -1);
        assertAnyValue(DESC, "a".getBytes(), "b".getBytes(), 1);
        assertAnyValue(ASC, "abc".getBytes(), "ab".getBytes(), 1);
        assertAnyValue(ASC, "abc".getBytes(), "abd".getBytes(), -1);
    }


    @Test
    void bytesAndStrings() {
        assertAnyValue(ASC, "", "".getBytes(), 0);
        assertAnyValue(ASC, "x", "x".getBytes(), 0);
        assertAnyValue(ASC, "a", "b".getBytes(), -1);
        assertAnyValue(DESC, "a", "b".getBytes(), 1);
        assertAnyValue(ASC, "abc", "ab".getBytes(), 1);
        assertAnyValue(ASC, "abc", "abd".getBytes(), -1);

        assertAnyValue(ASC, "".getBytes(), "", 0);
        assertAnyValue(ASC, "x".getBytes(), "x", 0);
        assertAnyValue(ASC, "a".getBytes(), "b", -1);
        assertAnyValue(DESC, "a".getBytes(), "b", 1);
        assertAnyValue(ASC, "abc".getBytes(), "ab", 1);
        assertAnyValue(ASC, "abc".getBytes(), "abd", -1);
    }

    @Test
    void numbersAndStrings() {
        assertAnyValue(ASC, 123, "123", 0);
        assertAnyValue(DESC, "456", 456, 0);
        assertAnyValue(DESC, "3.14", 3.14, 0);
        assertAnyValue(ASC, 3.14, "2.718281828", 1);
    }

    @Test
    void dates() {
        long now = System.currentTimeMillis();
        assertAnyValue(ASC, new Date(now), new Date(now), 0);
        assertAnyValue(DESC, new Date(now), new Date(now), 0);
        assertAnyValue(ASC, new Date(now + 1), new Date(now), 1);
    }

    @Test
    void sqlDates() {
        long now = System.currentTimeMillis();
        assertAnyValue(ASC, new java.sql.Date(now + 1), new java.sql.Date(now), 0);

        // Time resolution is seconds, so adding 1 millisecond might change the time if it was the last millisecond of current second or not to change otherwise
        int timeComp = new OrderItemsComparator<>(singletonList(new OrderItem("value", ASC)), (value, name) -> value).compare(new java.sql.Time(now + 1), new java.sql.Time(now));
        assertTrue(timeComp == 0 || timeComp > 0);


        assertAnyValue(ASC, new java.sql.Time(now + 1001), new java.sql.Time(now), 1); // 1001 ms is definitely enough to move to the next second.
        assertAnyValue(DESC, new java.sql.Timestamp(now + 1), new java.sql.Timestamp(now), -1);
    }


    @Test
    void mapsAscOrderByStringField() {
        assertEquals(Arrays.asList("George", "John", "Paul", "Ringo"), maps(new OrderItem("firstName")));
    }

    @Test
    void mapsOrderByYearOfBirthAndKidsCount() {
        assertEquals(Arrays.asList("John", "Ringo", "Paul", "George"), maps(new OrderItem("yearOfBirth"), new OrderItem("kidsCount")));
    }

    @Test
    void mapsOrderByYearOfBirthAscAndKidsCountDesc() {
        assertEquals(Arrays.asList("Ringo", "John", "Paul", "George"), maps(new OrderItem("yearOfBirth"), new OrderItem("kidsCount", DESC)));
    }


    private void assertIntValue(Direction direction, int one, int two, int expected) {
        assertEquals(expected, new OrderItemsComparator<>(singletonList(new OrderItem("value", direction)), (value, name) -> value).compare(one, two));
    }

    private <X, Y> void assertAnyValue(Direction direction, X one, Y two, int expected) {
        assertEquals(expected, new OrderItemsComparator<>(singletonList(new OrderItem("value", direction)), (value, name) -> value).compare(one, two));
    }

    private List<String> maps(OrderItem ... orderItems) {
        return peopleMap.stream().sorted(new OrderItemsComparator<>(Arrays.asList(orderItems), Map::get)).map(p -> (String)p.get("firstName")).collect(Collectors.toList());
    }
}