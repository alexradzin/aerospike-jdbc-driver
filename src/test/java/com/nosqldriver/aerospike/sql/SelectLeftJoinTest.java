package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.nosqldriver.TestUtils.getDisplayName;
import static com.nosqldriver.aerospike.sql.TestDataUtils.GUITARS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.KEYBOARDS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.conn;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeGuitars;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeKeyboards;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests of SELECT with LEFT JOIN
 */
class SelectLeftJoinTest {
    @BeforeAll
    static void init() {
        writeBeatles();
        writeGuitars();
    }

    @AfterAll
    static void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, GUITARS);
    }

    @AfterEach
    void clean() {
        deleteAllRecords(NAMESPACE, KEYBOARDS);
        deleteAllRecords(NAMESPACE, INSTRUMENTS);
    }


    @Test
    @DisplayName("select first_name, i.name as instrument from people as p join guitars as i on p.id=i.person_id")
    void joinGuitars() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("first_name", md.getColumnName(1));
        assertEquals("name", md.getColumnName(2));
        assertEquals("instrument", md.getColumnLabel(2));


        Map<String, Collection<String>> result = new HashMap<>();
        while(rs.next()) {
            assertEquals(rs.getString(1), rs.getString("first_name"));
            assertEquals(rs.getString(2), rs.getString("instrument"));
            String firstName = rs.getString(1);

            Collection<String> instruments = result.getOrDefault(firstName, new HashSet<>());
            instruments.add(rs.getString(2));
            result.put(firstName, instruments);
        }

        assertEquals(3, result.size());

        assertEquals(new HashSet<>(singleton("guitar")), result.get("John"));
        assertEquals(new HashSet<>(singleton("guitar")), result.get("Paul"));
        assertEquals(new HashSet<>(singleton("guitar")), result.get("George"));
    }

    @Test
    @DisplayName("select first_name, i.name as instrument from people as p left join guitars as i on p.id=i.person_id")
    void leftJoinGuitars() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("first_name", md.getColumnName(1));
        assertEquals("name", md.getColumnName(2));
        assertEquals("instrument", md.getColumnLabel(2));


        Map<String, Collection<String>> result = new HashMap<>();
        while(rs.next()) {
            assertEquals(rs.getString(1), rs.getString("first_name"));
            assertEquals(rs.getString(2), rs.getString("instrument"));
            String firstName = rs.getString(1);

            Collection<String> instruments = result.getOrDefault(firstName, new HashSet<>());
            String instrument = rs.getString(2);
            if (instrument != null) {
                instruments.add(instrument);
            }
            result.put(firstName, instruments);
        }

        assertEquals(4, result.size());

        assertEquals(new HashSet<>(singleton("guitar")), result.get("John"));
        assertEquals(new HashSet<>(singleton("guitar")), result.get("Paul"));
        assertEquals(new HashSet<>(singleton("guitar")), result.get("George"));
        assertEquals(new HashSet<>(), result.get("Ringo"));
    }


    @Test
    @DisplayName("select first_name, g.name as guitar, k.name as keyboards from people as p join guitars as g on p.id=g.person_id join keyboards as k on p.id=k.person_id")
    void joinGuitarsAndKeyboards() throws SQLException {
        writeKeyboards();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(3, md.getColumnCount());
        assertEquals("first_name", md.getColumnName(1));
        assertEquals("guitar", md.getColumnLabel(2));
        assertEquals("keyboards", md.getColumnLabel(3));


        Map<String, Collection<String>> result = new HashMap<>();
        while(rs.next()) {
            assertEquals(rs.getString(1), rs.getString("first_name"));
            assertEquals(rs.getString(2), rs.getString("guitar"));
            assertEquals(rs.getString(3), rs.getString("keyboards"));
            String firstName = rs.getString(1);

            Collection<String> instruments = result.getOrDefault(firstName, new HashSet<>());
            instruments.add(rs.getString(2));
            instruments.add(rs.getString(3));
            result.put(firstName, instruments);
        }

        assertEquals(2, result.size());
        System.out.println(result);

        assertEquals(new HashSet<>(asList("guitar", "keyboards")), result.get("John"));
        assertEquals(new HashSet<>(asList("guitar", "keyboards")), result.get("Paul"));
    }


}
