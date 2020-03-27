package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getClient;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class UpdateWithPreparedStatementTest {
    private IAerospikeClient client = getClient();

    @BeforeEach
    void init() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        writeBeatles();
    }
    @AfterAll
    static void cleanup() {
        deleteAllRecords(NAMESPACE, PEOPLE);
    }


    @Test
    void updateOneFieldSeveralRows() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertNull(rec.getString("band")); count.incrementAndGet();});
        assertEquals(4, count.get());

        executeUpdate("update people set band=?", new Object[] {"Beatles"}, 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals("Beatles", rec.getString("band")); count.incrementAndGet();}, "band");
        assertEquals(4, count.get());

        executeUpdate("update people set band=?", new Object[] {"The Beatles"}, 4);
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

        executeUpdate("update people set kids_count=0 where id=?", new Object[] {1}, 1);
        Map<Integer, Integer> kidsCount2 = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount2.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(0, kidsCount2.get(1).intValue());

        Map<Integer, Integer> kidsCount3 = new HashMap<>();
        executeUpdate("update people set kids_count=? where id=?", new Object[] {2, 1}, 1);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount3.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(expectedKidsCount, kidsCount3);
    }

    @Test
    void updateOneFieldOneRowByPK() throws SQLException {
        writeBeatles();
        Map<Integer, Integer> expectedKidsCount = Arrays.stream(beatles).collect(toMap(Person::getId, Person::getKidsCount));

        Map<Integer, Integer> kidsCount1 = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount1.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(expectedKidsCount, kidsCount1);

        executeUpdate("update people set kids_count=0 where PK=?", new Object[] {1}, 1);
        Map<Integer, Integer> kidsCount2 = new HashMap<>();
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount2.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(0, kidsCount2.get(1).intValue());

        Map<Integer, Integer> kidsCount3 = new HashMap<>();
        executeUpdate("update people set kids_count=? where PK=?", new Object[] {2, 1}, 1);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {kidsCount3.put(rec.getInt("id"), rec.getInt("kids_count"));});
        assertEquals(expectedKidsCount, kidsCount3);
    }


    @Test
    void updateSeveralFieldsSeveralRows() throws SQLException {
        writeBeatles();

        AtomicInteger count = new AtomicInteger(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertNull(rec.getString("band")); assertNull(rec.getString("occupation")); count.incrementAndGet();});
        assertEquals(4, count.get());

        executeUpdate("update people set band=?, occupation=?", new Object[] {"Beatles", "musician"}, 4);
        count.set(0);
        client.scanAll(null, NAMESPACE, PEOPLE, (key, rec) -> {assertEquals("Beatles", rec.getString("band")); assertEquals("musician", rec.getString("occupation")); count.incrementAndGet();}, "band", "occupation");
        assertEquals(4, count.get());
    }


    private void executeUpdate(String sql, Object[] params, int expectedRowCount) throws SQLException {
        PreparedStatement ps = getTestConnection().prepareStatement(sql);
        for (int i = 0; i < params.length; i++) {
            ps.setObject(i + 1, params[i]);
        }
        assertEquals(expectedRowCount, ps.executeUpdate());
    }
}
