package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.conn;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeAllPersonalInstruments;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

/**
 * Tests of SELECT with JOIN
 */
class SelectJoinTest {
    @BeforeAll
    static void init() {
        writeBeatles();
        writeAllPersonalInstruments();
    }

    @AfterAll
    static void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, INSTRUMENTS);
    }





    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id",
            "select first_name, i.name as instrument from people as p inner join instruments as i on p.id=i.person_id",
            "select first_name, i.name as instrument from people as p left join instruments as i on p.id=i.person_id",
    })
    void oneToManyJoin(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
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

        assertEquals(4, result.size());

        assertEquals(new HashSet<>(asList("vocals", "guitar", "keyboards", "harmonica")), result.get("John"));
        assertEquals(new HashSet<>(asList("vocals", "bass guitar", "guitar", "keyboards")), result.get("Paul"));
        assertEquals(new HashSet<>(asList("vocals", "guitar", "sitar")), result.get("George"));
        assertEquals(new HashSet<>(asList("vocals", "drums")), result.get("Ringo"));
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='John'",
    })
    void oneToManyJoinWhereMainTable(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
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

        assertEquals(1, result.size());
        assertEquals(new HashSet<>(asList("vocals", "guitar", "keyboards", "harmonica")), result.get("John"));
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where instrument='guitar'",
    })
    void oneToManyJoinWhereSecondaryTable(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
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

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='Paul' and i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where p.first_name='Paul' and i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where p.first_name='Paul' and instrument='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='Paul' and instrument='guitar'",
    })
    void oneToManyJoinWhereMainAndSecondaryTable(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
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

        assertEquals(1, result.size());

        assertEquals(new HashSet<>(singleton("guitar")), result.get("Paul"));
    }
}
