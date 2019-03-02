package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelectTest {
    private static final String NAMESPACE = "test";
    private static final String SET = "people";

    private final AerospikeClient client = new AerospikeClient("localhost", 3000);
    private Connection conn;

    private static final Person[] beatles = new Person[] {
            new Person(1, "John", "Lennon", 1940),
            new Person(2, "Paul", "McCartney", 1942),
            new Person(3, "George", "Harrison", 1943),
            new Person(4, "Ringo", "Starr", 1940),
    };


    private static class Person {
        private final int id;
        private final String firstName;
        private final String lastName;
        private final int yearOfBirth;

        public Person(int id, String firstName, String lastName, int yearOfBirth) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.yearOfBirth = yearOfBirth;
        }
    }

    @BeforeEach
    void init() throws SQLException {
        conn = DriverManager.getConnection("jdbc:aerospike:localhost/test");
        assertNotNull(conn);
    }

    @BeforeEach
    @AfterEach
    void dropAll() {
        client.scanAll(new ScanPolicy(), NAMESPACE, SET, (key, record) -> client.delete(new WritePolicy(), key));
        dropIndexSafely("first_name");
        dropIndexSafely("year_of_birth");
    }

    private void dropIndexSafely(String fieldName) {
        try {
            dropIndex(fieldName);
        } catch (AerospikeException e) {
            if (e.getResultCode() != 201) {
                throw e;
            }
        }
    }

    @Test
    void selectEmpty() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("select * from people");
        assertFalse(rs.next());
    }


    @Test
    void selectAll() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select * from people");
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

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
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select first_name, year_of_birth from people");

        Map<String, Integer> selectedPeople = new HashMap<>();

        assertEquals("first_name", rs.getMetaData().getColumnName(1));
        assertEquals("year_of_birth", rs.getMetaData().getColumnName(2));
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("first_name"), rs.getInt("year_of_birth"));
            assertNull(rs.getString("last_name"));
            assertNull(rs.getString("id"));
        }

        assertEquals(1940, selectedPeople.get("John").intValue());
        assertEquals(1942, selectedPeople.get("Paul").intValue());
        assertEquals(1943, selectedPeople.get("George").intValue());
        assertEquals(1940, selectedPeople.get("Ringo").intValue());
    }

    @Test
    void selectByPk() throws SQLException {
        writeBeatles();
        for (int i = 0; i < 4; i++) {
            int id = i + 1;
            ResultSet rs = conn.createStatement().executeQuery("select * from people where PK=" + id);
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

            assertTrue(rs.next());
            assertEquals(beatles[i].id, rs.getInt("id"));
            assertEquals(beatles[i].firstName, rs.getString("first_name"));
            assertEquals(beatles[i].lastName, rs.getString("last_name"));
            assertEquals(beatles[i].yearOfBirth, rs.getInt("year_of_birth"));
        }
    }


    @Test
    void selectByOneStringIndexedField() throws SQLException {
        writeBeatles();
        createIndex("first_name", IndexType.STRING);
        for (int i = 0; i < 4; i++) {
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where first_name=%s", beatles[i].firstName));
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

            assertTrue(rs.next());
            assertEquals(beatles[i].id, rs.getInt("id"));
            assertEquals(beatles[i].firstName, rs.getString("first_name"));
            assertEquals(beatles[i].lastName, rs.getString("last_name"));
            assertEquals(beatles[i].yearOfBirth, rs.getInt("year_of_birth"));
        }
    }

    @Test
    @DisplayName("year_of_birth=1942 -> [Paul], year_of_birth=1943 -> [George]")
    void selectOneRecordByOneNumericIndexedFieldEq() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, "=", 1942, 2);
        selectByOneNumericIndexedField(conn, "=", 1943, 3);
    }

    @Test
    @DisplayName("year_of_birth=1940 -> [John, Ringo]")
    void selectSeveralRecordsByOneNumericIndexedFieldEq() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, "=", 1940, 1, 4);
    }

    @Test
    @DisplayName("year_of_birth=1939 -> nothing")
    void selectNothingByOneNumericIndexedFieldEq() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, "=", 1939);
    }

    @Test
    @DisplayName("year_of_birth>1939 -> [John, Paul, George, Ringo]")
    void selectAllRecordsByOneNumericIndexedFieldGt() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, ">", 1939, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("year_of_birth>=1940 -> [John, Paul, George, Ringo]")
    void selectAllRecordsByOneNumericIndexedFieldGe() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, ">=", 1940, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("year_of_birth>1940 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldGe() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, ">", 1940, 2, 3);
    }

    @Test
    @DisplayName("year_of_birth<1939 -> nothing")
    void selectNothingRecordsByOneNumericIndexedFieldLt() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, "<", 1939);
    }

    @Test
    @DisplayName("year_of_birth<1940 -> nothing")
    void selectNothingRecordsByOneNumericIndexedFieldLt2() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, "<", 1940);
    }

    @Test
    @DisplayName("year_of_birth<=1940 -> [John, Ringo]")
    void selectNothingRecordsByOneNumericIndexedFieldLe() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        selectByOneNumericIndexedField(conn, "<=", 1940, 1, 4);
    }

    private void selectByOneNumericIndexedField(Connection conn, String operation, int year, int ... expectedIds) throws SQLException {
        select(conn, format("select * from people where year_of_birth%s%s", operation, year), expectedIds);
    }

    private void select(Connection conn, String sql, int ... expectedIds) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        assertPeople(rs, beatles, expectedIds);
    }

    private void assertPeople(ResultSet rs, Person[] people, int ... expectedIds) throws SQLException {
        Set<Integer> expectedIdsSet = new HashSet<>();
        Arrays.stream(expectedIds).forEach(expectedIdsSet::add);

        int n = 0;
        for (; n < expectedIds.length; n++) {
            assertTrue(rs.next());
            int id = rs.getInt("id");
            assertTrue(expectedIdsSet.contains(id));
            int i = id - 1;
            assertEquals(people[i].id, rs.getInt("id"));
            assertEquals(people[i].firstName, rs.getString("first_name"));
            assertEquals(people[i].lastName, rs.getString("last_name"));
            assertEquals(people[i].yearOfBirth, rs.getInt("year_of_birth"));
        }
        assertEquals(expectedIds.length, n);
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

    private void createIndex(String fieldName, IndexType indexType) {
        client.createIndex(null, NAMESPACE, SET, getIndexName(fieldName), fieldName, indexType).waitTillComplete();
    }

    private void dropIndex(String fieldName) {
        client.dropIndex(null, NAMESPACE, SET, getIndexName(fieldName)).waitTillComplete();
    }

    private String getIndexName(String fieldName) {
        return format("%s_%s_INDEX", SET, fieldName.toUpperCase());
    }

    private void writeBeatles() {
        WritePolicy writePolicy = new WritePolicy();
        write(writePolicy, 1, person(1, "John", "Lennon", 1940));
        write(writePolicy, 2, person(2, "Paul", "McCartney", 1942));
        write(writePolicy, 3, person(3, "George", "Harrison", 1943));
        write(writePolicy, 4, person(4, "Ringo", "Starr", 1940));
    }

}
