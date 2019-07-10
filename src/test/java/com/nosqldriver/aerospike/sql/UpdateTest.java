package com.nosqldriver.aerospike.sql;

import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.client;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

abstract class UpdateTest {

    @BeforeEach
    void init() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        writeBeatles();
    }

    @AfterAll
    static void cleanup() {
        deleteAllRecords(NAMESPACE, PEOPLE);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "update people set band='Beatles'",
            "update people set band='Beatles' where PK=1",
            "update people set band='Beatles' where PK in (1, 2, 3, 4)",
            "update people set band='Beatles' where PK>=1 and PK <= 4",
            "update people set band='Beatles' where id = 1",
    })
    void updateEmptyDb(String sql) throws SQLException {
        deleteAllRecords(NAMESPACE, PEOPLE);
        // check that DB is empty
        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> count.incrementAndGet());
        assertEquals(0, count.get());

        executeUpdate(sql, 0);
        // check that DB is still empty
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> count.incrementAndGet());
        assertEquals(0, count.get());
    }


    @Test
    void updateOneFieldSeveralRows() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertNull(rec.getString("band")); count.incrementAndGet();});
        assertEquals(4, count.get());

        executeUpdate("update people set band='Beatles'", 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals("Beatles", rec.getString("band")); count.incrementAndGet();}, "band");
        assertEquals(4, count.get());

        executeUpdate("update people set band='The Beatles'", 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals("The Beatles", rec.getString("band")); count.incrementAndGet();}, "band");
        assertEquals(4, count.get());
    }

    @Test
    void updateOneFieldOneRow() throws SQLException {
        writeBeatles();
        Map<Integer, Integer> expectedKidsCount = Arrays.stream(beatles).collect(Collectors.toMap(Person::getId, Person::getKidsCount));

        Map<Integer, Integer> kidsCount1 = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount1.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(expectedKidsCount, kidsCount1);

        executeUpdate("update people set kids_count=0 where id=1", 1);
        Map<Integer, Integer> kidsCount2 = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount2.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(0, kidsCount2.get(1).intValue());

        Map<Integer, Integer> kidsCount3 = new HashMap<>();
        executeUpdate("update people set kids_count=2 where id=1", 1);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount3.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(expectedKidsCount, kidsCount3);
    }


    @Test
    void updateSeveralFieldsSeveralRows() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertNull(rec.getString("band")); assertNull(rec.getString("occupation")); count.incrementAndGet();});
        assertEquals(4, count.get());

        executeUpdate("update people set band='Beatles', occupation='musician'", 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals("Beatles", rec.getString("band")); assertEquals("musician", rec.getString("occupation")); count.incrementAndGet();}, "band", "occupation");
        assertEquals(4, count.get());
    }

    @Test
    void updateCopyFieldToFieldSeveralRows() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertNull(rec.getString("given_name")); count.incrementAndGet();});
        assertEquals(4, count.get());

        executeUpdate("update people set given_name=first_name", 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals(rec.getString("first_name"), rec.getString("given_name")); count.incrementAndGet();});
        assertEquals(4, count.get());
    }

    @Test
    void updateCalculateColumn() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger nullsCount = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {if(rec.getValue("age") == null) nullsCount.incrementAndGet(); count.incrementAndGet();});
        assertEquals(4, count.get());
        assertEquals(4, nullsCount.get());

        executeUpdate("update people set age=year()-year_of_birth", 4);
        count.set(0);

        int currentYear = Calendar.getInstance().get(Calendar.YEAR);
        Map<String, Integer> expectedAges = Arrays.stream(beatles).collect(Collectors.toMap(Person::getFirstName, p -> currentYear - p.getYearOfBirth()));

        Map<String, Integer> actualAges = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {actualAges.put(rec.getString("first_name"), Double.valueOf(rec.getDouble("age")).intValue()); count.incrementAndGet();});
        assertEquals(4, count.get());
        assertEquals(expectedAges, actualAges);
    }

    protected abstract void executeUpdate(String sql, int expectedRowCount) throws SQLException;

}
