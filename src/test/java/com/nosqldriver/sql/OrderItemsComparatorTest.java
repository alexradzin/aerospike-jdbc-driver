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
        intValue(ASC, 123, 123, 0);
        intValue(DESC, 456, 456, 0);
    }

    @Test
    void intGt() {
        intValue(ASC, 456, 123, 1);
        intValue(DESC, 456, 123, -1);
    }

    @Test
    void intLt() {
        intValue(ASC, 123, 456, -1);
        intValue(DESC, 123, 456, 1);

        anyValue(ASC, 123, null, 1);
        anyValue(DESC, 0, null, -1);
    }

    @Test
    void shortLt() {
        intValue(ASC, (short)123, (short)456, -1);
        intValue(DESC, (short)123, (short)456, 1);
    }

    @Test
    void byteLt() {
        intValue(ASC, (byte)12, (byte)34, -1);
        intValue(DESC, (byte)12, (byte)34, 1);
        anyValue(DESC, null, (byte)1, 1);
    }


    @Test
    void numbersOfDifferentTypes() {
        anyValue(ASC, 123L, 456, -1);
        anyValue(DESC, 123, 456L, 1);
        anyValue(DESC, (short)123, 123L, 0);
    }

    @Test
    void floatingPointNumbersOfDifferentTypes() {
        anyValue(ASC, 3.14f, 2.1718281828, 1);
        anyValue(DESC, 3.14, 2.1718281828f, -1);
        anyValue(ASC, 3.14f, 3, 1);
        anyValue(ASC, 3.14, 4, -1);
        anyValue(DESC, 3, 2.7, -1);
        anyValue(ASC, 4, 3.14, 1);
    }

    @Test
    void booleans() {
        anyValue(ASC, false, true, -1);
        anyValue(ASC, true, true, 0);
        anyValue(ASC, true, false, 1);

        anyValue(DESC, false, true, 1);
        anyValue(DESC, true, true, 0);
        anyValue(DESC, true, false, -1);

        anyValue(DESC, true, "true", 0);
        anyValue(ASC, true, "false", 1);
        anyValue(DESC, "true", "true", 0);
        anyValue(ASC, "true", true, 0);
    }


    @Test
    void strings() {
        anyValue(ASC, "", "", 0);
        anyValue(ASC, "x", "x", 0);
        anyValue(ASC, "a", "b", -1);
        anyValue(DESC, "a", "b", 1);
        anyValue(ASC, "abc", "ab", 1);
        anyValue(ASC, "abc", "abd", -1);
    }

    @Test
    void bytes() {
        anyValue(ASC, "".getBytes(), "".getBytes(), 0);
        anyValue(ASC, "x".getBytes(), "x".getBytes(), 0);
        anyValue(ASC, "a".getBytes(), "b".getBytes(), -1);
        anyValue(DESC, "a".getBytes(), "b".getBytes(), 1);
        anyValue(ASC, "abc".getBytes(), "ab".getBytes(), 1);
        anyValue(ASC, "abc".getBytes(), "abd".getBytes(), -1);
    }


    @Test
    void bytesAndStrings() {
        anyValue(ASC, "", "".getBytes(), 0);
        anyValue(ASC, "x", "x".getBytes(), 0);
        anyValue(ASC, "a", "b".getBytes(), -1);
        anyValue(DESC, "a", "b".getBytes(), 1);
        anyValue(ASC, "abc", "ab".getBytes(), 1);
        anyValue(ASC, "abc", "abd".getBytes(), -1);

        anyValue(ASC, "".getBytes(), "", 0);
        anyValue(ASC, "x".getBytes(), "x", 0);
        anyValue(ASC, "a".getBytes(), "b", -1);
        anyValue(DESC, "a".getBytes(), "b", 1);
        anyValue(ASC, "abc".getBytes(), "ab", 1);
        anyValue(ASC, "abc".getBytes(), "abd", -1);
    }

    @Test
    void numbersAndStrings() {
        anyValue(ASC, 123, "123", 0);
        anyValue(DESC, "456", 456, 0);
        anyValue(DESC, "3.14", 3.14, 0);
        anyValue(ASC, 3.14, "2.718281828", 1);
    }

    void dates() {
        long now = System.currentTimeMillis();
        anyValue(ASC, new Date(now), new Date(now), 0);
        anyValue(DESC, new Date(now), new Date(now), 0);
        anyValue(ASC, new Date(now + 1), new Date(now), 1);
    }

    @Test
    void sqlDates() {
        long now = System.currentTimeMillis();
        anyValue(ASC, new java.sql.Date(now + 1), new java.sql.Date(now), 0);

        // Time resolution is seconds, so adding 1 millisecond might change the time if it was the last millisecond of current second or not to change otherwise
        int timeComp = new OrderItemsComparator<>(singletonList(new OrderItem("value", ASC)), (value, name) -> value).compare(new java.sql.Time(now + 1), new java.sql.Time(now));
        assertTrue(timeComp == 0 || timeComp > 0);


        anyValue(ASC, new java.sql.Time(now + 1001), new java.sql.Time(now), 1); // 1001 ms is definitely enough to move to the next second.
        anyValue(DESC, new java.sql.Timestamp(now + 1), new java.sql.Timestamp(now), -1);
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


    private void intValue(Direction direction, int one, int two, int expected) {
        assertEquals(expected, new OrderItemsComparator<>(singletonList(new OrderItem("value", direction)), (value, name) -> value).compare(one, two));
    }

    private <X, Y> void anyValue(Direction direction, X one, Y two, int expected) {
        assertEquals(expected, new OrderItemsComparator<>(singletonList(new OrderItem("value", direction)), (value, name) -> value).compare(one, two));
    }

    private List<String> maps(OrderItem ... orderItems) {
        return peopleMap.stream().sorted(new OrderItemsComparator<>(Arrays.asList(orderItems), Map::get)).map(p -> (String)p.get("firstName")).collect(Collectors.toList());
    }
}