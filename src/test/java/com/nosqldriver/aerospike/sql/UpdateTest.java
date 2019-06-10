package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class UpdateTest {
    private static final String NAMESPACE = "test";
    private static final String PEOPLE = "people";
    private Connection conn;
    private final AerospikeClient client = new AerospikeClient("localhost", 3000);

    @BeforeEach
    void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
        assertNotNull(conn);
    }

    @BeforeEach
    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
    }

    private void deleteAllRecords(String namespace, String table) {
        client.scanAll(new ScanPolicy(), namespace, table, (key, record) -> client.delete(new WritePolicy(), key));
    }


    private void writeBeatles() {
        WritePolicy writePolicy = new WritePolicy();
        write(PEOPLE, writePolicy, 1, person(1, "John", "Lennon", 1940, 2));
        write(PEOPLE, writePolicy, 2, person(2, "Paul", "McCartney", 1942, 5));
        write(PEOPLE, writePolicy, 3, person(3, "George", "Harrison", 1943, 1));
        write(PEOPLE, writePolicy, 4, person(4, "Ringo", "Starr", 1940, 3));
    }

    private void write(WritePolicy writePolicy, Key key, Bin... bins) {
        client.put(writePolicy, key, bins);
    }

    private void write(String table, WritePolicy writePolicy, int id, Bin ... bins) {
        write(writePolicy, new Key(NAMESPACE, table, id), bins);
    }

    private Bin[] person(int id, String firstName, String lastName, int yearOfBirth, int kidsCount) {
        return new Bin[] {new Bin("id", id), new Bin("first_name", firstName), new Bin("last_name", lastName), new Bin("year_of_birth", yearOfBirth), new Bin("kids_count", kidsCount)};
    }


    @Test
    void updateOneRow() throws SQLException {
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




    void executeUpdate(String sql, int expectedRowCount) throws SQLException {
        int rowCount = conn.createStatement().executeUpdate(sql);
        assertEquals(expectedRowCount, rowCount);
    }
}
