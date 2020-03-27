package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.nosqldriver.TestUtils.getDisplayName;
import static com.nosqldriver.aerospike.sql.TestDataUtils.DATA;
import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SELECT_ALL;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SUBJECT_SELECTION;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.write;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeData;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeMainPersonalInstruments;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeSubjectSelection;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

/**
 * Additional tests that verify SELECT SQL statement.
 * This test case includes tests that were not included into {@link SelectTest} because they need special data in DB.
 * Each test implemented here is responsible on filling required data, {@link #dropAll()} method that run after each
 * test must clean all data.
  */
class SpecialSelectTest {
    private Connection testConn = getTestConnection();

    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, INSTRUMENTS);
        deleteAllRecords(NAMESPACE, SUBJECT_SELECTION);
        deleteAllRecords(NAMESPACE, DATA);
    }

    @Test
    void selectEmpty() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(SELECT_ALL);
        assertFalse(rs.next());
    }


    @Test
    @DisplayName("select subject, semester, count(*) from subject_selection group by subject, semester")
    void groupByMulti() throws SQLException {
        writeBeatles();
        writeSubjectSelection();
        ResultSet rs = testConn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(3, md.getColumnCount());
        assertEquals("subject", md.getColumnName(1));
        assertEquals("semester", md.getColumnName(2));
        assertEquals("count(*)", md.getColumnName(3));



        // {ITB001,2={count(*)=2}, MKB114,1={count(*)=2}, ITB001,1={count(*)=3}}
        Map<String, Integer> expected = new HashMap<>();
        expected.put("ITB001,2", 2);
        expected.put("MKB114,1", 2);
        expected.put("ITB001,1", 3);

        Collection<String> groups = new HashSet<>();
        assertTrue(rs.next());
        groups.add(assertSubjectSelection(rs, expected));
        assertTrue(rs.next());
        groups.add(assertSubjectSelection(rs, expected));
        assertTrue(rs.next());
        groups.add(assertSubjectSelection(rs, expected));
        assertEquals(expected.keySet(), groups);

        assertFalse(rs.next());
    }


    private String assertSubjectSelection(ResultSet rs, Map<String, Integer> expected) throws SQLException {
        String subject = rs.getString(1);
        int semester = rs.getInt(2);
        int count = rs.getInt(3);
        String group = subject + "," + semester;
        assertTrue(expected.containsKey(group));
        assertEquals(expected.get(group).intValue(), count);
        return group;
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id",
            "select first_name, i.name as instrument from people as p inner join instruments as i on p.id=i.person_id",
            "select first_name, i.name as instrument from people as p left join instruments as i on p.id=i.person_id",

            "select first_name, i.name as instrument from instruments as i join people as p on p.id=i.person_id",
            "select first_name, i.name as instrument from instruments as i inner join people as p on p.id=i.person_id",
            "select first_name, i.name as instrument from instruments as i left join people as p on p.id=i.person_id",

            "select first_name, i.name as instrument from instruments as i join people as p on i.person_id=p.PK",
            "select first_name, i.name as instrument from instruments as i inner join people as p on i.person_id=p.PK",
            "select first_name, i.name as instrument from instruments as i left join people as p on i.person_id=p.PK",
    })
    void oneToOneJoin(String sql) throws SQLException {
        writeBeatles();
        writeMainPersonalInstruments();
        ResultSet rs = testConn.createStatement().executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("first_name", md.getColumnName(1));
        assertEquals("name", md.getColumnName(2));
        assertEquals("instrument", md.getColumnLabel(2));


        Map<String, String> result = new HashMap<>();
        while(rs.next()) {
            assertEquals(rs.getString(1), rs.getString("first_name"));
            assertEquals(rs.getString(2), rs.getString("instrument"));
            result.put(rs.getString(1), rs.getString(2));
        }

        assertEquals(4, result.size());
        assertEquals("guitar", result.get("John"));
        assertEquals("bass guitar", result.get("Paul"));
        assertEquals("guitar", result.get("George"));
        assertEquals("drums", result.get("Ringo"));
    }


    @Test
    void stringPK() throws SQLException {
        writeData();
        ResultSet rs = testConn.createStatement().executeQuery("select * from data where PK='one'");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    void groupByDouble() throws SQLException {
        WritePolicy writePolicy = new WritePolicy();
        write(writePolicy, new Key(NAMESPACE, DATA, "pi"), new Bin("name", "PI"), new Bin("value", 3.14));
        write(writePolicy, new Key(NAMESPACE, DATA, "e"), new Bin("name", "EXP"), new Bin("value", 2.7));

        ResultSet rs = testConn.createStatement().executeQuery("select value, count(*) from data group by value");

        Map<Double, Integer> actual = new HashMap<>();
        while(rs.next()) {
            actual.put(rs.getDouble(1), rs.getInt(2));
        }
        assertFalse(rs.next());
        Map<Double, Integer> expected = new HashMap<>();
        expected.put(3.14, 1);
        expected.put(2.7, 1);

        assertEquals(expected, actual);
        rs.close();
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select text from data;abc,abcd,bcd",
            "select text from data where text like '%c%';abc,abcd,bcd",
            "select text from data where text like 'a%';abc,abcd",
            "select text from data where text like '%a%';abc,abcd",
            "select text from data where text like '%c';abc",
            "select text from data where text like '%d';abcd,bcd",
            "select text from data where text like '%bcd';abcd,bcd",
            "select text from data where text like 'a';",
            "select text from data where text like '%a';",
    })
    void selectLike(String queryAndResult) throws SQLException {
        String[] parts = queryAndResult.split(";");
        String sql = parts[0];
        Collection<String> expected = parts.length == 1 ? Collections.emptySet() : new HashSet<>(Arrays.asList(parts[1].split(",")));
        testConn.createStatement().executeUpdate("insert into data (PK, text) values (1, 'abc'), (2, 'abcd'), (3, 'bcd')");

        ResultSet rs = testConn.createStatement().executeQuery(sql);
        Collection<String> actual  = new HashSet<>();
        while(rs.next()) {
            actual.add(rs.getString(1));
        }
        assertEquals(expected, actual);
    }
}
