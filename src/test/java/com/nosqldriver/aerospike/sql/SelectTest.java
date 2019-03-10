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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SelectTest {
    private static final String NAMESPACE = "test";
    private static final String SET = "people";
    private static final String SELECT_ALL = "select * from people";

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

        private Person(int id, String firstName, String lastName, int yearOfBirth) {
            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.yearOfBirth = yearOfBirth;
        }
    }

    private Function<String, Integer> executeUpdate = new Function<String, Integer>() {
        @Override
        public Integer apply(String sql) {
            try {
                return conn.createStatement().executeUpdate(sql);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    };

    private Function<String, Boolean> execute = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String sql) {
            try {
                return conn.createStatement().execute(sql);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    };

    private Function<String, ResultSet> executeQuery = new Function<String, ResultSet>() {
        @Override
        public ResultSet apply(String sql) {
            try {
                return conn.createStatement().executeQuery(sql);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    };


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
        ResultSet rs = conn.createStatement().executeQuery(SELECT_ALL);
        assertFalse(rs.next());
    }


    @Test
    void selectAll() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(SELECT_ALL);
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
    void selectSpecificFieldsWithAliases() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select first_name as name, year_of_birth as year from people");

        Map<String, Integer> selectedPeople = new HashMap<>();

        assertEquals("first_name", rs.getMetaData().getColumnName(1));
        assertEquals("year_of_birth", rs.getMetaData().getColumnName(2));
        assertEquals("name", rs.getMetaData().getColumnLabel(1));
        assertEquals("year", rs.getMetaData().getColumnLabel(2));
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("name"), rs.getInt("year"));
            assertNull(rs.getString("last_name"));
            assertNull(rs.getString("id"));
        }

        assertEquals(1940, selectedPeople.get("John").intValue());
        assertEquals(1942, selectedPeople.get("Paul").intValue());
        assertEquals(1943, selectedPeople.get("George").intValue());
        assertEquals(1940, selectedPeople.get("Ringo").intValue());
    }

    @Test
    @DisplayName("select 1 as one from people where PK=1")
    void select1fromPeople() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select 1 as one from people where PK=1");
        assertTrue(rs.next());
        assertEquals("one", rs.getMetaData().getColumnLabel(1));
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("one"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("select (1+2)*3 as nine from people where PK=1")
    void selectIntExpressionFromPeople() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select (1+2)*3 as nine from people where PK=1");
        assertTrue(rs.next());
        assertEquals("nine", rs.getMetaData().getColumnLabel(1));
        assertEquals(9, rs.getInt(1));
        assertEquals(9, rs.getInt("nine"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("select (4+5)/3 as three, first_name as name, year_of_birth - 1900 as year from people where PK=1")
    void selectExpressionAndFieldFromPeople() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select (4+5)/3 as three, first_name as name, year_of_birth - 1900 as year from people where PK=1");
        assertTrue(rs.next());
        assertEquals("three", rs.getMetaData().getColumnLabel(1));
        assertEquals(3, rs.getInt(1));
        assertEquals(3, rs.getInt("three"));

        assertEquals("name", rs.getMetaData().getColumnLabel(2));
        assertEquals("John", rs.getString(2));
        assertEquals("John", rs.getString("name"));

        assertEquals("year", rs.getMetaData().getColumnLabel(3));
        assertEquals(40, rs.getInt(3));
        assertEquals(40, rs.getInt("year"));

        assertFalse(rs.next());
    }

    @Test
    void selectSpecificFieldsWithLowerCaseLenFunction() throws SQLException {
        selectSpecificFieldsWithLenFunction("select first_name as name, len(first_name) as name_len from people");
    }

    @Test
    void selectSpecificFieldsWithUpperCaseLenFunction() throws SQLException {
        selectSpecificFieldsWithLenFunction("select first_name as name, LEN(first_name) as name_len from people");
    }

    private void selectSpecificFieldsWithLenFunction(String query) throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(query);

        Map<String, Integer> selectedPeople = new HashMap<>();

        assertEquals("first_name", rs.getMetaData().getColumnName(1));
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("name"), rs.getInt("name_len"));
        }

        selectedPeople.forEach((key, value) -> assertEquals(value.intValue(), key.length()));
    }


    //@Test
    void selectSpecificFieldsWithFunctions2() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select 1 one, 2 + 3 as two_plus_three, (1+2)*3 as someexpr, year() - year_of_birth as years_ago, first_name as name, year_of_birth - 1900 as y, length(last_name) as surname_length from people");

        Map<String, Integer> selectedPeople = new HashMap<>();

        assertEquals("first_name", rs.getMetaData().getColumnName(1));
        assertEquals("name", rs.getMetaData().getColumnLabel(1));
        assertEquals("surname_length", rs.getMetaData().getColumnLabel(2));
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("name"), rs.getInt("surname_length"));
            assertNull(rs.getString("last_name"));
            assertNull(rs.getString("id"));
        }

        assertEquals("Lennon".length(), selectedPeople.get("John").intValue());
        assertEquals("McCartney".length(), selectedPeople.get("Paul").intValue());
        assertEquals("Harrison".length(), selectedPeople.get("George").intValue());
        assertEquals("Starr".length(), selectedPeople.get("Ringo").intValue());
    }



    @Test
    void selectByPk() throws SQLException {
        writeBeatles();
        for (int i = 0; i < 4; i++) {
            int id = i + 1;
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where PK=%d", id));
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

            assertTrue(rs.next());
            assertEquals(beatles[i].id, rs.getInt("id"));
            assertEquals(beatles[i].firstName, rs.getString("first_name"));
            assertEquals(beatles[i].lastName, rs.getString("last_name"));
            assertEquals(beatles[i].yearOfBirth, rs.getInt("year_of_birth"));
        }
    }

    @Test
    void selectByNotIndexedField() throws SQLException {
        writeBeatles();
        for (int i = 0; i < 4; i++) {
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where last_name=%s", beatles[i].lastName));
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
    @DisplayName("year_of_birth=1940 and first_name='John'-> [John]")
    void selectOneRecordByOneNumericIndexedFieldEqAndOneNotIndexedField() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        select(conn, "select * from people where year_of_birth=1940 and first_name='John'", 1);
    }

    @Test
    @DisplayName("year_of_birth=1940 and last_name='Lennon'-> [John]")
    void selectOneRecordByOneNumericEqAndOneStringFieldAllNotIndexed() throws SQLException {
        writeBeatles();
        select(conn, "select * from people where year_of_birth=1940 and last_name='Lennon'", 1);
    }


    @Test
    @DisplayName("last_name='Lennon' or last_name='Harrison' -> [John, George]")
    void selectSeveralPersonsByLastNameOr() throws SQLException {
        writeBeatles();
        select(conn, "select * from people where last_name='Lennon' or last_name='Harrison'", 1, 3);
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
    @DisplayName("year_of_birth between 1942 and 1943 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldBetween() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        select(conn, "select * from people where year_of_birth between 1942 and 1943", 2, 3);
    }

    @Test
    @DisplayName("year_of_birth between 1941 and 1944 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldBetween2() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        select(conn, "select * from people where year_of_birth between 1941 and 1944", 2, 3);
    }


    @Test
    @DisplayName("PK IN (2, 3) -> [Paul, George]")
    void selectSeveralRecordsByPkIn() throws SQLException {
        writeBeatles();
        select(conn, "select * from people where PK in (2, 3)", 2, 3);
    }

    @Test
    @DisplayName("PK IN (20, 30) -> []")
    void selectNoRecordsByPkIn() throws SQLException {
        writeBeatles();
        select(conn, "select * from people where PK in (20, 30)");
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

    @Test
    @DisplayName("limit 2 -> [John, Paul]")
    void selectAllWithLimit2() throws SQLException {
        writeBeatles();
        select(conn, "select * from people limit 2", 1, 2);

        ResultSet rs = conn.createStatement().executeQuery("select * from people limit 2");
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        int n = 0;
        //noinspection StatementWithEmptyBody // counter
        for (; rs.next(); n++);
        assertEquals(2, n);
    }

    @Test
    @DisplayName("offset 1 -> [Paul, George, Ringo]")
    void selectAllWithOffset1() throws SQLException {
        writeBeatles();
        String sql = "select * from people offset 1";
        //select(conn, sql, 2, 3, 4);

        // since order of records returned from the DB is not deterministic unless order by is used we cannot really
        // validate here the names of people and ought to check the number of rows in record set.
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        int n = 0;
        //noinspection StatementWithEmptyBody // counter
        for (; rs.next(); n++);
        assertEquals(3, n);
    }


    @Test
    void deleteAll() throws SQLException {
        delete(executeUpdate, "delete from people", p -> false, res -> res == 4);
        delete(execute, "delete from people", p -> false, res -> res);
        delete(executeQuery, "delete from people", p -> false, rs -> !resultSetNext(rs));
    }


    @Test
    void deleteByPkEq() throws SQLException {
        delete(executeUpdate, "delete from people where PK=1", p -> !"John".equals(p.firstName), res -> res == 1);
        delete(execute, "delete from people where PK=1", p -> !"John".equals(p.firstName), res -> res);
        delete(executeQuery, "delete from people where PK=1", p -> !"John".equals(p.firstName), rs -> !resultSetNext(rs));
    }


    @Test
    void deleteByPkIn() throws SQLException {
        delete(executeUpdate, "delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.firstName), res -> res == 3);
        delete(execute, "delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.firstName), res -> res);
        delete(executeQuery, "delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.firstName), rs -> !resultSetNext(rs));
    }

    @Test
    void deleteByPkBetween() throws SQLException {
        delete(executeUpdate, "delete from people where PK between 1 and 3", p -> "Ringo".equals(p.firstName), res -> res == 3);
        delete(execute, "delete from people where PK between 1 and 3", p -> "Ringo".equals(p.firstName), res -> res);
        delete(executeQuery, "delete from people where PK between 1 and 3", p -> "Ringo".equals(p.firstName), rs -> !resultSetNext(rs));
    }

    @Test
    void deleteByCriteria() throws SQLException {
        delete(executeUpdate, "delete from people where year_of_birth=1940", p -> p.yearOfBirth != 1940, res -> res == 2);
        delete(execute, "delete from people where year_of_birth=1940", p -> p.yearOfBirth != 1940, res -> res);
        delete(executeQuery, "delete from people where year_of_birth=1940", p -> p.yearOfBirth != 1940, rs -> !resultSetNext(rs));
    }

    private <T> void delete(Function<String, T> executor, String deleteSql, Predicate<Person> expectedResultFilter, Predicate<T> returnValueValidator) throws SQLException {
        writeBeatles();
        Collection<String> names1 = retrieveColumn("select * from people", "first_name");
        assertEquals(Arrays.stream(beatles).map(p -> p.firstName).collect(Collectors.toSet()), names1);

        assertTrue(returnValueValidator.test(executor.apply(deleteSql)));

        Collection<String> names2 = retrieveColumn("select * from people", "first_name");
        assertEquals(Arrays.stream(beatles).filter(expectedResultFilter).map(p -> p.firstName).collect(Collectors.toSet()), names2);
    }

    boolean resultSetNext(ResultSet rs) {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }


    private Collection<String> retrieveColumn(String sql, String column) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        Collection<String> data = new HashSet<>();
        while (rs.next()) {
            data.add(rs.getString(column));
        }
        return data;
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
            assertTrue(expectedIdsSet.contains(id), "ID " + id + " is unexpected" );
            int i = id - 1;
            assertEquals(people[i].id, rs.getInt("id"));
            assertEquals(people[i].firstName, rs.getString("first_name"));
            assertEquals(people[i].lastName, rs.getString("last_name"));
            assertEquals(people[i].yearOfBirth, rs.getInt("year_of_birth"));
        }
        assertEquals(expectedIds.length, n);
        assertFalse(rs.next());
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
