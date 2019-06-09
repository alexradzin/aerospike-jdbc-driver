package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InsertTest {
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

    @Test
    void insertOneRow() throws SQLException {
        insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("year_of_birth", 1940L);
        expectedData.put("kids_count", 2L);
        assertEquals(expectedData, record.bins);
    }

    @Test
    void insertDuplicateRow() throws SQLException {
        insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("year_of_birth", 1940L);
        expectedData.put("kids_count", 2L);
        assertEquals(expectedData, record.bins);

        SQLException e = assertThrows(SQLException.class, () -> {
            insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        });
        assertTrue(e.getMessage().contains("Duplicate entries"));
    }

    @Test
    void insertIgnoreDuplicateRow() throws SQLException {
        insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        assertEquals("John", record.getString("first_name"));

        insert("insert ignore into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2)", 1);
        assertNotNull(record);
        assertEquals("John", record.getString("first_name"));
    }



    @Test
    void insertSeveralRows() throws SQLException {
        insert("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2), (2, 2, 'Paul', 'McCartney', 1942, 5)", 2);
        assertEquals("John", client.get(null, new Key("test", "people", 1)).getString("first_name"));
        assertEquals("Paul", client.get(null, new Key("test", "people", 2)).getString("first_name"));
    }

    @Test
    void insertSeveralRowsUsingExecute() throws SQLException {
        assertTrue(conn.createStatement().execute("insert into people (PK, id, first_name, last_name, year_of_birth, kids_count) values (1, 1, 'John', 'Lennon', 1940, 2), (2, 2, 'Paul', 'McCartney', 1942, 5)"));
        assertEquals("John", client.get(null, new Key("test", "people", 1)).getString("first_name"));
        assertEquals("Paul", client.get(null, new Key("test", "people", 2)).getString("first_name"));
    }



    void insert(String sql, int expectedRowCount) throws SQLException {
        int rowCount = conn.createStatement().executeUpdate(sql);
        assertEquals(expectedRowCount, rowCount);
    }
}
