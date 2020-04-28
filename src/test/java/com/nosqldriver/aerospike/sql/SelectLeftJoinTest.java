package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static com.nosqldriver.TestUtils.getDisplayName;
import static com.nosqldriver.aerospike.sql.SelectJoinTest.collect;
import static com.nosqldriver.aerospike.sql.SelectJoinTest.guitar;
import static com.nosqldriver.aerospike.sql.TestDataUtils.GUITARS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.KEYBOARDS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeGuitars;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeKeyboards;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of SELECT with LEFT JOIN
 */
class SelectLeftJoinTest {
    @BeforeAll
    static void init() {
        dropAll();
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
        ResultSet rs = executeQuery(getDisplayName(), NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "instrument", VARCHAR);
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "instrument");
        assertEquals(3, result.size());
        Collection<String> guitar  = new HashSet<>(singleton("guitar"));
        asList("John", "Paul", "George").forEach(name -> assertEquals(guitar, result.get(name)));
    }

    @Test
    @DisplayName("select first_name, i.name as instrument from people as p left join guitars as i on p.id=i.person_id")
    void leftJoinGuitars() throws SQLException {
        ResultSet rs = executeQuery(getDisplayName(), NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "instrument", VARCHAR);
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "instrument");
        assertEquals(4, result.size());
        asList("John", "Paul", "George").forEach(name -> assertEquals(guitar, result.get(name)));
        assertEquals(new HashSet<>(), result.get("Ringo"));
    }


    @Test
    @DisplayName("select first_name, g.name as guitar, k.name as keyboards from people as p join guitars as g on p.id=g.person_id join keyboards as k on p.id=k.person_id")
    void joinGuitarsAndKeyboards() throws SQLException {
        writeKeyboards();
        ResultSet rs = executeQuery(getDisplayName(), NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "guitar", VARCHAR,  "name" , "keyboards", VARCHAR);
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "guitar", "keyboards");
        assertEquals(2, result.size());

        Collection<String> guitarAndKeyboards = new HashSet<>(asList("guitar", "keyboards"));
        asList("John", "Paul").forEach(name -> assertEquals(guitarAndKeyboards, result.get(name)));
    }
}
