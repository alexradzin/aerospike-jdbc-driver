package com.nosqldriver.aerospike.sql;

import com.nosqldriver.Person;
import org.junit.Assert;
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
import java.util.function.Function;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.client;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

            "update test.people set band='Beatles'",
            "update test.people set band='Beatles' where PK=1",
            "update test.people set band='Beatles' where PK in (1, 2, 3, 4)",
            "update test.people set band='Beatles' where PK>=1 and PK <= 4",
            "update test.people set band='Beatles' where id = 1",

            "update people set band='Beatles' where id in (1, 2, 3, 4)",
            "update people set band='Beatles' where id in (1, 2); update people set band='Beatles' where id in (3, 4)",

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
        Map<Integer, Integer> expectedKidsCount = Arrays.stream(beatles).collect(toMap(Person::getId, Person::getKidsCount));

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

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "update people set band='Beatles', occupation='musician'",
            "update people set band='Beatles', occupation='musician' where PK IN (1,2,3,4)",
            "update people set band='Beatles', occupation='musician' where PK IN (1,3); update people set band='Beatles', occupation='musician' where PK IN (2,4)",
            "update people set band='Beatles', occupation='musician' where PK IN (1,2,3); update people set band='Beatles', occupation='musician' where PK IN (4)",
    })
    void updateSeveralFieldsSeveralRows(String sql) throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertNull(rec.getString("band")); assertNull(rec.getString("occupation")); count.incrementAndGet();});
        assertEquals(4, count.get());

        executeUpdate(sql, 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals("Beatles", rec.getString("band")); assertEquals("musician", rec.getString("occupation")); count.incrementAndGet();}, "band", "occupation");
        assertEquals(4, count.get());
    }

    @Test
    void updateCalculateColumn() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        AtomicInteger nullsCount = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {if (rec.getValue("age") == null) nullsCount.incrementAndGet(); count.incrementAndGet();});
        assertEquals(4, count.get());
        assertEquals(4, nullsCount.get());

        executeUpdate("update people set age=year()-year_of_birth", 4);
        count.set(0);

        executeUpdate("update people set age_days=age*365", 4);
        count.set(0);

        executeUpdate("update people set age_month=age/12", 4);
        count.set(0);

        executeUpdate("update people set year_of_century=(year_of_birth-1900)", 4);
        count.set(0);

        executeUpdate("update people set year=year_of_birth+1900", 4);
        count.set(0);

        executeUpdate("update people set pi=3.14", 4);
        count.set(0);

        executeUpdate("update people set absolute_zero=-273", 4);
        count.set(0);

        int currentYear = Calendar.getInstance().get(Calendar.YEAR);
        assertEquals(Arrays.stream(beatles).collect(toMap(Person::getFirstName, p -> currentYear - p.getYearOfBirth())), retrieveData("first_name", "age", o -> ((Number)o).intValue()));

        assertEquals(Arrays.stream(beatles).collect(toMap(Person::getFirstName, p -> (currentYear - p.getYearOfBirth())*365)), retrieveData("first_name", "age_days", o -> ((Number)o).intValue()));
        assertEquals(Arrays.stream(beatles).collect(toMap(Person::getFirstName, p -> (currentYear - p.getYearOfBirth())/12)), retrieveData("first_name", "age_month", o -> ((Number)o).intValue()));
        assertEquals(Arrays.stream(beatles).collect(toMap(Person::getFirstName, p -> (p.getYearOfBirth()) - 1900)), retrieveData("first_name", "year_of_century", o -> ((Number)o).intValue()));
        assertEquals(Arrays.stream(beatles).collect(toMap(Person::getFirstName, p -> 3.14)), retrieveData("first_name", "pi", o -> (Double)o));
        assertEquals(Arrays.stream(beatles).collect(toMap(Person::getFirstName, p -> -273)), retrieveData("first_name", "absolute_zero", o -> ((Number)o).intValue()));

        executeUpdate("update people set absolute_zero=NULL", 4);
        count.set(0);
        retrieveData("first_name", "absolute_zero", o -> o).values().forEach(Assert::assertNull);
    }

    @Test
    void updateSeveralTables() {
        assertEquals("Update statement can proceed one table only but was 2", assertThrows(SQLException.class, () -> executeUpdate("update one, two set something=1", 0)).getMessage());
    }

    private <T> Map<String, T> retrieveData(String keyName, String valueName, Function<Object, T> typeTransformer) {
        Map<String, T> actualValues = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {actualValues.put(rec.getString(keyName), typeTransformer.apply(rec.getValue(valueName)));});
        return actualValues;
    }


    protected abstract void executeUpdate(String sql, int expectedRowCount) throws SQLException;

}
