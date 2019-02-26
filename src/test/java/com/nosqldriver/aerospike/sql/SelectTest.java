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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SelectTest {
    private static final String NAMESPACE = "test";
    private static final String SET = "people";

    private final AerospikeClient client = new AerospikeClient("localhost", 3000);

    @BeforeEach
    @AfterEach
    void dropAll() {
        client.scanAll(new ScanPolicy(), NAMESPACE, SET, (key, record) -> client.delete(new WritePolicy(), key));
    }


    @Test
    void selectAll() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
        assertNotNull(conn);

        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select * from people");

        Map<Integer, String> selectedPeople = new HashMap<>();
        while (rs.next()) {
            selectedPeople.put(rs.getInt("id"), rs.getString("first_name") + " " + rs.getString("last_name") + " " + rs.getInt("year_of_birth"));
        }

        assertEquals("John Lennon 1940", selectedPeople.get(1));
        assertEquals("Paul McCartney 1942", selectedPeople.get(2));
        assertEquals("George Harrison 1943", selectedPeople.get(3));
        assertEquals("Ringo Starr 1940", selectedPeople.get(4));
    }


    @Test
    void selectSpecificFields() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
        assertNotNull(conn);

        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select first_name, year_of_birth from people");

        Map<String, Integer> selectedPeople = new HashMap<>();
        while (rs.next()) {
            selectedPeople.put(rs.getString("first_name"), rs.getInt("year_of_birth"));
        }

        assertEquals(1940, selectedPeople.get("John").intValue());
        assertEquals(1942, selectedPeople.get("Paul").intValue());
        assertEquals(1943, selectedPeople.get("George").intValue());
        assertEquals(1940, selectedPeople.get("Ringo").intValue());
    }


    private Bin[] person(int id, String firstName, String lastName, int yearOfBirth) {
        return new Bin[] {new Bin("id", id), new Bin("first_name", firstName), new Bin("last_name", lastName), new Bin("year_of_birth", yearOfBirth)};
    }

    private void write(WritePolicy writePolicy, Key key, Bin ... bins) {
        client.put(writePolicy, key, bins);
    }

    private void write(WritePolicy writePolicy, int id, Bin ... bins) {
        write(writePolicy, new Key(NAMESPACE, SET, id), bins);
    }

    private void writeBeatles() {
        WritePolicy writePolicy = new WritePolicy();
        write(writePolicy, 1, person(1, "John", "Lennon", 1940));
        write(writePolicy, 2, person(2, "Paul", "McCartney", 1942));
        write(writePolicy, 3, person(3, "George", "Harrison", 1943));
        write(writePolicy, 4, person(4, "Ringo", "Starr", 1940));
    }

}
