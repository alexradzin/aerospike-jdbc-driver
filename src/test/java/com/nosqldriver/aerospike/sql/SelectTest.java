package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.TestUtils.getDisplayName;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

class SelectTest {
    private static final String NAMESPACE = "test";
    private static final String PEOPLE = "people";
    private static final String INSTRUMENTS = "instruments";
    private static final String SELECT_ALL = "select * from people";

    private final AerospikeClient client = new AerospikeClient("localhost", 3000);
    private Connection conn;

    private static final Person[] beatles = new Person[] {
            new Person(1, "John", "Lennon", 1940, 2),
            new Person(2, "Paul", "McCartney", 1942, 5),
            new Person(3, "George", "Harrison", 1943, 1),
            new Person(4, "Ringo", "Starr", 1940, 3),
    };


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
        client.scanAll(new ScanPolicy(), NAMESPACE, PEOPLE, (key, record) -> client.delete(new WritePolicy(), key));
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


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            SELECT_ALL,
            "select * from people as p"
    })
    void selectAll(String sql) throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(sql);
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

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            SELECT_ALL,
            "select * from people as p"
    })
    void selectAllWithPreparedStatement(String sql) throws SQLException {
        writeBeatles();
        ResultSet rs = conn.prepareStatement(sql).executeQuery();
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


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, year_of_birth from people",
            "select people.first_name, people.year_of_birth from people",
            "select first_name, year_of_birth from people as p",
            "select p.first_name, year_of_birth from people as p",
            "select first_name, p.year_of_birth from people as p",
            "select p.first_name, p.year_of_birth from people as p",
            "select p.first_name, p.year_of_birth from people as p",
    })
    void selectSpecificFields(String sql) throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(sql);

        Map<String, Integer> selectedPeople = new HashMap<>();

        assertEquals("first_name", rs.getMetaData().getColumnName(1));
        assertEquals("year_of_birth", rs.getMetaData().getColumnName(2));
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("first_name"), rs.getInt("year_of_birth"));
            assertThrowsSqlException(() -> rs.getString("last_name"), "last_name");
            assertThrowsSqlException(() -> rs.getString("id"), "id");
        }

        assertEquals(1940, selectedPeople.get("John").intValue());
        assertEquals(1942, selectedPeople.get("Paul").intValue());
        assertEquals(1943, selectedPeople.get("George").intValue());
        assertEquals(1940, selectedPeople.get("Ringo").intValue());
    }

    @Test
    @DisplayName("select first_name as name, year_of_birth as year from people")
    void selectSpecificFieldsWithAliases() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());

        Map<String, Integer> selectedPeople = new HashMap<>();

        assertEquals("first_name", rs.getMetaData().getColumnName(1));
        assertEquals("year_of_birth", rs.getMetaData().getColumnName(2));
        assertEquals("name", rs.getMetaData().getColumnLabel(1));
        assertEquals("year", rs.getMetaData().getColumnLabel(2));
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("name"), rs.getInt("year"));
            assertThrowsSqlException(() -> rs.getString("last_name"), "last_name");
            assertThrowsSqlException(() -> rs.getString("id"), "id");
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
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        assertTrue(rs.next());
        assertEquals("one", rs.getMetaData().getColumnLabel(1));
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("one"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("select 1 as one from people where PK=?")
    void select1fromPeopleUsingPreparedStatement() throws SQLException {
        writeBeatles();
        PreparedStatement ps = conn.prepareStatement(getDisplayName());
        ps.setInt(1, 1);
        ResultSet rs = ps.executeQuery();
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
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        assertTrue(rs.next());
        assertEquals("nine", rs.getMetaData().getColumnLabel(1));
        assertEquals(9, rs.getInt(1));
        assertEquals(9, rs.getInt("nine"));
        assertFalse(rs.next());
    }


    @Test
    @DisplayName("select (1+2)*3 as nine")
    void selectIntExpressionNoFrom() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
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
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
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



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name as name, len(first_name) as name_len from people",
            "select first_name as name, LEN(first_name) as name_len from people"
    })
    void selectSpecificFieldsWithLenFunction(String query) throws SQLException {
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


    @Test
    void selectSpecificFieldsWithFunctions2() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery("select 1 as one, 2 + 3 as two_plus_three, (1+2)*3 as someexpr, year() - year_of_birth as years_ago, first_name as name, year_of_birth - 1900 as y, len(last_name) as surname_length from people");

        Map<String, Integer> selectedPeople = new HashMap<>();

        ResultSetMetaData md = rs.getMetaData();
        String[] expectedLabels = new String[] {"one", "two_plus_three", "someexpr", "years_ago", "name", "y", "surname_length"};
        for (int i = 0; i < expectedLabels.length; i++) {
            assertEquals(expectedLabels[i], md.getColumnLabel(i + 1));
        }
        String[] expectedNames = new String[] {null, null, null, null, "first_name", null, null};
        for (int i = 0; i < expectedNames.length; i++) {
            assertEquals(expectedNames[i], md.getColumnName(i + 1));
        }

        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        while (rs.next()) {
            selectedPeople.put(rs.getString("name"), rs.getInt("surname_length"));

            assertThrowsSqlException(() -> rs.getString("last_name"), "last_name");
            assertThrowsSqlException(() -> rs.getString("id"), "id");
        }

        assertEquals("Lennon".length(), selectedPeople.get("John").intValue());
        assertEquals("McCartney".length(), selectedPeople.get("Paul").intValue());
        assertEquals("Harrison".length(), selectedPeople.get("George").intValue());
        assertEquals("Starr".length(), selectedPeople.get("Ringo").intValue());
    }


    private void assertThrowsSqlException(Executable getCall, String columnName) {
        assertThrows(SQLException.class, getCall, format("Column '%s' not found", columnName));
    }

    @Test
    void selectByPk() throws SQLException {
        writeBeatles();
        for (int i = 0; i < beatles.length; i++) {
            int id = i + 1;
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where PK=%d", id));
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

            assertTrue(rs.next());
            assertEquals(beatles[i].getId(), rs.getInt("id"));
            assertEquals(beatles[i].getFirstName(), rs.getString("first_name"));
            assertEquals(beatles[i].getLastName(), rs.getString("last_name"));
            assertEquals(beatles[i].getYearOfBirth(), rs.getInt("year_of_birth"));
        }
    }

    @Test
    void selectByNotIndexedField() throws SQLException {
        writeBeatles();
        for (Person person : beatles) {
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where last_name=%s", person.getLastName()));
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

            assertTrue(rs.next());
            assertEquals(person.getId(), rs.getInt("id"));
            assertEquals(person.getFirstName(), rs.getString("first_name"));
            assertEquals(person.getLastName(), rs.getString("last_name"));
            assertEquals(person.getYearOfBirth(), rs.getInt("year_of_birth"));
        }
    }

    @Test
    void selectByOneStringIndexedField() throws SQLException {
        writeBeatles();
        createIndex("first_name", IndexType.STRING);
        for (Person person : beatles) {
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where first_name=%s", person.getFirstName()));
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));

            assertTrue(rs.next());
            assertEquals(person.getId(), rs.getInt("id"));
            assertEquals(person.getFirstName(), rs.getString("first_name"));
            assertEquals(person.getLastName(), rs.getString("last_name"));
            assertEquals(person.getYearOfBirth(), rs.getInt("year_of_birth"));
        }
    }

    @Test
    @DisplayName("year_of_birth=1942 -> [Paul], year_of_birth=1943 -> [George]")
    void selectOneRecordByOneNumericIndexedFieldEq() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, "=", 1942, 2);
        assertSelectByOneNumericIndexedField(conn, "=", 1943, 3);
    }

    @Test
    @DisplayName("year_of_birth=1940 -> [John, Ringo]")
    void selectSeveralRecordsByOneNumericIndexedFieldEq() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, "=", 1940, 1, 4);
    }

    @Test
    @DisplayName("year_of_birth=1940 and first_name='John'-> [John]")
    void selectOneRecordByOneNumericIndexedFieldEqAndOneNotIndexedField() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect(conn, "select * from people where year_of_birth=1940 and first_name='John'", 1);
    }


    @Test
    @DisplayName("year_of_birth=1940 and first_name='John'-> [John]")
    void selectOneRecordByOneNumericIndexedFieldEqAndOneNotIndexedFieldUsingPreparedStatement() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect(conn, "select * from people where year_of_birth=1940 and first_name='John'", 1);

        PreparedStatement ps = conn.prepareStatement("select * from people where year_of_birth=? and first_name=?");
        ps.setInt(1, 1940);
        ps.setString(2, "John");
        ResultSet rs = ps.executeQuery();
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        assertPeople(rs, beatles, 1);
    }


    @Test
    @DisplayName("year_of_birth=1940 and last_name='Lennon'-> [John]")
    void selectOneRecordByOneNumericEqAndOneStringFieldAllNotIndexed() throws SQLException {
        writeBeatles();
        assertSelect(conn, "select * from people where year_of_birth=1940 and last_name='Lennon'", 1);
    }


    @Test
    @DisplayName("last_name='Lennon' or last_name='Harrison' -> [John, George]")
    void selectSeveralPersonsByLastNameOr() throws SQLException {
        writeBeatles();
        assertSelect(conn, "select * from people where last_name='Lennon' or last_name='Harrison'", 1, 3);
    }


    @Test
    @DisplayName("year_of_birth=1939 -> nothing")
    void selectNothingByOneNumericIndexedFieldEq() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, "=", 1939);
    }

    @Test
    @DisplayName("year_of_birth>1939 -> [John, Paul, George, Ringo]")
    void selectAllRecordsByOneNumericIndexedFieldGt() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, ">", 1939, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("year_of_birth>=1940 -> [John, Paul, George, Ringo]")
    void selectAllRecordsByOneNumericIndexedFieldGe() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, ">=", 1940, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("year_of_birth>1940 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldGe() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, ">", 1940, 2, 3);
    }

    @Test
    @DisplayName("year_of_birth between 1942 and 1943 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldBetween() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect(conn, "select * from people where year_of_birth between 1942 and 1943", 2, 3);
    }

    @Test
    @DisplayName("year_of_birth between 1941 and 1944 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldBetween2() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect(conn, "select * from people where year_of_birth between 1941 and 1944", 2, 3);
    }


    @Test
    @DisplayName("PK IN (2, 3) -> [Paul, George]")
    void selectSeveralRecordsByPkIn() throws SQLException {
        writeBeatles();
        assertSelect(conn, "select * from people where PK in (2, 3)", 2, 3);
    }

    @Test
    @DisplayName("PK IN (20, 30) -> []")
    void selectNoRecordsByPkIn() throws SQLException {
        writeBeatles();
        assertSelect(conn, "select * from people where PK in (20, 30)");
    }


    @Test
    @DisplayName("year_of_birth<1939 -> nothing")
    void selectNothingRecordsByOneNumericIndexedFieldLt() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, "<", 1939);
    }

    @Test
    @DisplayName("year_of_birth<1940 -> nothing")
    void selectNothingRecordsByOneNumericIndexedFieldLt2() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, "<", 1940);
    }

    @Test
    @DisplayName("year_of_birth<=1940 -> [John, Ringo]")
    void selectNothingRecordsByOneNumericIndexedFieldLe() throws SQLException {
        writeBeatles();
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(conn, "<=", 1940, 1, 4);
    }

    @Test
    @DisplayName("limit 2 -> [John, Paul]")
    void selectAllWithLimit2() throws SQLException {
        writeBeatles();
        assertSelect(conn, "select * from people limit 2", 1, 2);

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
        assertDelete(executeUpdate, "delete from people", p -> false, res -> res == 4);
        assertDelete(execute, "delete from people", p -> false, res -> res);
        assertDelete(executeQuery, "delete from people", p -> false, rs -> !resultSetNext(rs));
    }


    @Test
    void deleteByPkEq() throws SQLException {
        assertDelete(executeUpdate, "delete from people where PK=1", p -> !"John".equals(p.getFirstName()), res -> res == 1);
        assertDelete(execute, "delete from people where PK=1", p -> !"John".equals(p.getFirstName()), res -> res);
        assertDelete(executeQuery, "delete from people where PK=1", p -> !"John".equals(p.getFirstName()), rs -> !resultSetNext(rs));
    }


    @Test
    void deleteByPkIn() throws SQLException {
        assertDelete(executeUpdate, "delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.getFirstName()), res -> res == 3);
        assertDelete(execute, "delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.getFirstName()), res -> res);
        assertDelete(executeQuery, "delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.getFirstName()), rs -> !resultSetNext(rs));
    }

    @Test
    void deleteByPkBetween() throws SQLException {
        assertDelete(executeUpdate, "delete from people where PK between 1 and 3", p -> "Ringo".equals(p.getFirstName()), res -> res == 3);
        assertDelete(execute, "delete from people where PK between 1 and 3", p -> "Ringo".equals(p.getFirstName()), res -> res);
        assertDelete(executeQuery, "delete from people where PK between 1 and 3", p -> "Ringo".equals(p.getFirstName()), rs -> !resultSetNext(rs));
    }

    @Test
    void deleteByCriteria() throws SQLException {
        assertDelete(executeUpdate, "delete from people where year_of_birth=1940", p -> p.getYearOfBirth() != 1940, res -> res == 2);
        assertDelete(execute, "delete from people where year_of_birth=1940", p -> p.getYearOfBirth() != 1940, res -> res);
        assertDelete(executeQuery, "delete from people where year_of_birth=1940", p -> p.getYearOfBirth() != 1940, rs -> !resultSetNext(rs));
    }


    @Test
    @DisplayName("select count(*) from people")
    void countAll() throws SQLException {
        assertAggregateOneField(getDisplayName(), "count(*)", "count(*)", 4);
    }

    @Test
    @DisplayName("select count(*) as number_of_people from people")
    void countAllWithAlias() throws SQLException {
        assertAggregateOneField(getDisplayName(), "count(*)", "number_of_people", 4);
    }

    @Test
    @DisplayName("select count(year_of_birth) from people")
    void countYearOfBirth() throws SQLException {
        assertAggregateOneField(getDisplayName(), "count(year_of_birth)", "count(year_of_birth)", 4);
    }

    @Test
    @DisplayName("select count(year_of_birth) as n from people")
    void countYearOfBirthWithAlias() throws SQLException {
        assertAggregateOneField(getDisplayName(), "count(year_of_birth)", "n", 4);
    }

    @Test
    @DisplayName("select max(year_of_birth) as youngest from people")
    void maxYearOfBirh() throws SQLException {
        assertAggregateOneField(getDisplayName(), "max(year_of_birth)", "youngest", 1943);
    }

    @Test
    @DisplayName("select min(year_of_birth) as oldest from people")
    void minYearOfBirth() throws SQLException {
        assertAggregateOneField(getDisplayName(), "min(year_of_birth)", "oldest", 1940);
    }

    @Test
    @DisplayName("select count(*) as n, min(year_of_birth) as min, max(year_of_birth) as max, avg(year_of_birth) as avg, sum(year_of_birth) as total from people")
    void callAllAggregations() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(NAMESPACE, md.getSchemaName(1));
        assertEquals(5, rs.getMetaData().getColumnCount());

        assertEquals("count(*)", md.getColumnName(1));
        assertEquals("n", md.getColumnLabel(1));
        assertEquals("min(year_of_birth)", md.getColumnName(2));
        assertEquals("min", md.getColumnLabel(2));
        assertEquals("max(year_of_birth)", md.getColumnName(3));
        assertEquals("max", md.getColumnLabel(3));
        assertEquals("avg(year_of_birth)", md.getColumnName(4));
        assertEquals("avg", md.getColumnLabel(4));
        assertEquals("sum(year_of_birth)", md.getColumnName(5));
        assertEquals("total", md.getColumnLabel(5));

        assertTrue(rs.next());

        assertEquals(4, rs.getInt(1));
        assertEquals(4, rs.getInt("n"));
        assertEquals(1940, rs.getInt(2));
        assertEquals(1940, rs.getInt("min"));
        assertEquals(1943, rs.getInt(3));
        assertEquals(1943, rs.getInt("max"));
        double average = stream(beatles).mapToInt(p -> p.getYearOfBirth()).average().orElseThrow(() -> new IllegalStateException("No average found"));
        assertEquals(average, rs.getDouble(4), 0.001);
        assertEquals(average, rs.getDouble("avg"), 0.001);
        int sum = stream(beatles).mapToInt(p -> p.getYearOfBirth()).sum();
        assertEquals(sum, rs.getInt(5));
        assertEquals(sum, rs.getInt("total"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("select distinct(year_of_birth) as year from people")
    void selectDistinctYearOfBirth() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(NAMESPACE, md.getSchemaName(1));
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("distinct(year_of_birth)", rs.getMetaData().getColumnName(1));
        assertEquals("year", rs.getMetaData().getColumnLabel(1));

        Collection<Integer> years = new LinkedHashSet<>();
        while(rs.next()) {
            years.add(rs.getInt(1));
        }
        assertEquals(new HashSet<>(Arrays.asList(1940, 1942, 1943)), years);
    }

    @Test
    @DisplayName("select distinct(first_name) as given_name from people")
    void selectDistinctFirstName() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(NAMESPACE, md.getSchemaName(1));
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals("distinct(first_name)", rs.getMetaData().getColumnName(1));
        assertEquals("given_name", rs.getMetaData().getColumnLabel(1));

        Collection<String> names = new HashSet<>();
        while(rs.next()) {
            names.add(rs.getString(1));
        }
        assertEquals(stream(beatles).map(p -> p.getFirstName()).collect(Collectors.toSet()), names);
    }

    @Test
    @DisplayName("select year_of_birth, count(*) from people group by year_of_birth")
    void groupByYearOfBirth() throws SQLException {
        assertGroupByYearOfBirth(getDisplayName());
    }

    @Test
    @DisplayName("select year_of_birth as year, count(*) as n from people group by year_of_birth")
    void groupByYearOfBirthWithAliases() throws SQLException {
        ResultSetMetaData md = assertGroupByYearOfBirth(getDisplayName());
        assertEquals("year", md.getColumnLabel(1));
        assertEquals("n", md.getColumnLabel(2));
    }


    private ResultSetMetaData assertGroupByYearOfBirth(String sql) throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("year_of_birth", md.getColumnName(1));
        assertEquals("count(*)", md.getColumnName(2));

        assertTrue(rs.next());
        assertEquals(1940, rs.getInt(1));
        assertEquals(2, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1942, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertTrue(rs.next());
        assertEquals(1943, rs.getInt(1));
        assertEquals(1, rs.getInt(2));
        assertFalse(rs.next());
        return md;
    }


    @Test
    @DisplayName("select year_of_birth as year, sum(kids_count) as total_kids from people group by year_of_birth")
    void groupByYearOfBirthWithAggregation() throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("year_of_birth", md.getColumnName(1));
        assertEquals("sum(kids_count)", md.getColumnName(2));
        assertEquals("total_kids", md.getColumnLabel(2));

        assertTrue(rs.next());
        assertCounts(rs, 1940);
        assertTrue(rs.next());
        assertCounts(rs, 1942);
        assertTrue(rs.next());
        assertCounts(rs, 1943);

        assertFalse(rs.next());
    }


    @Test
    @DisplayName("select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id")
    void simpleJoin() throws SQLException {
        writeBeatles();
        writeMainPersonalInstruments();
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
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

    private void assertCounts(ResultSet rs, int yearOfBirth) throws SQLException {
        assertEquals(yearOfBirth, rs.getInt(1));
        assertEquals(sum(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(2), 0.01);
    }

    private <T> int sum(Stream<T> stream, Predicate<T> filter, ToIntFunction<T> pf) {
        return stream.filter(filter).mapToInt(pf).sum();
    }

    void assertAggregateOneField(String sql, String name, String label, int expected) throws SQLException {
        writeBeatles();
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        assertEquals(1, rs.getMetaData().getColumnCount());
        assertEquals(name, rs.getMetaData().getColumnName(1));
        assertEquals(label, rs.getMetaData().getColumnLabel(1));
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
        assertEquals(expected, rs.getInt(label));
        assertFalse(rs.next());
    }

    private <T> void assertDelete(Function<String, T> executor, String deleteSql, Predicate<Person> expectedResultFilter, Predicate<T> returnValueValidator) throws SQLException {
        writeBeatles();
        Collection<String> names1 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).map(Person::getFirstName).collect(Collectors.toSet()), names1);

        assertTrue(returnValueValidator.test(executor.apply(deleteSql)));

        Collection<String> names2 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).filter(expectedResultFilter).map(Person::getFirstName).collect(Collectors.toSet()), names2);
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

    private void assertSelectByOneNumericIndexedField(Connection conn, String operation, int year, int ... expectedIds) throws SQLException {
        assertSelect(conn, format("select * from people where year_of_birth%s%s", operation, year), expectedIds);
    }

    private void assertSelect(Connection conn, String sql, int ... expectedIds) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        assertPeople(rs, beatles, expectedIds);
    }

    private void assertPeople(ResultSet rs, Person[] people, int ... expectedIds) throws SQLException {
        Set<Integer> expectedIdsSet = new HashSet<>();
        stream(expectedIds).forEach(expectedIdsSet::add);

        int n = 0;
        for (; n < expectedIds.length; n++) {
            assertTrue(rs.next());
            int id = rs.getInt("id");
            assertTrue(expectedIdsSet.contains(id), "ID " + id + " is unexpected" );
            int i = id - 1;
            assertEquals(people[i].getId(), rs.getInt("id"));
            assertEquals(people[i].getFirstName(), rs.getString("first_name"));
            assertEquals(people[i].getLastName(), rs.getString("last_name"));
            assertEquals(people[i].getYearOfBirth(), rs.getInt("year_of_birth"));
        }
        assertEquals(expectedIds.length, n);
        assertFalse(rs.next());
    }


    private Bin[] person(int id, String firstName, String lastName, int yearOfBirth, int kidsCount) {
        return new Bin[] {new Bin("id", id), new Bin("first_name", firstName), new Bin("last_name", lastName), new Bin("year_of_birth", yearOfBirth), new Bin("kids_count", kidsCount)};
    }

    private Bin[] personalInstrument(int id, int personId, String name) {
        return new Bin[] {new Bin("id", id), new Bin("person_id", personId), new Bin("name", name)};
    }


    private void write(WritePolicy writePolicy, Key key, Bin ... bins) {
        client.put(writePolicy, key, bins);
    }

    private void write(String table, WritePolicy writePolicy, int id, Bin ... bins) {
        write(writePolicy, new Key(NAMESPACE, table, id), bins);
    }

    private void createIndex(String fieldName, IndexType indexType) {
        client.createIndex(null, NAMESPACE, PEOPLE, getIndexName(fieldName), fieldName, indexType).waitTillComplete();
    }

    private void dropIndex(String fieldName) {
        client.dropIndex(null, NAMESPACE, PEOPLE, getIndexName(fieldName)).waitTillComplete();
    }

    private String getIndexName(String fieldName) {
        return format("%s_%s_INDEX", PEOPLE, fieldName.toUpperCase());
    }

    private void writeBeatles() {
        WritePolicy writePolicy = new WritePolicy();
        write(PEOPLE, writePolicy, 1, person(1, "John", "Lennon", 1940, 2));
        write(PEOPLE, writePolicy, 2, person(2, "Paul", "McCartney", 1942, 5));
        write(PEOPLE, writePolicy, 3, person(3, "George", "Harrison", 1943, 1));
        write(PEOPLE, writePolicy, 4, person(4, "Ringo", "Starr", 1940, 3));
    }

    private void writeMainPersonalInstruments() {
        WritePolicy writePolicy = new WritePolicy();
        write(INSTRUMENTS, writePolicy, 1, personalInstrument(1, 1, "guitar"));
        write(INSTRUMENTS, writePolicy, 2, personalInstrument(2, 2, "bass guitar"));
        write(INSTRUMENTS, writePolicy, 3, personalInstrument(3, 3, "guitar"));
        write(INSTRUMENTS, writePolicy, 4, personalInstrument(4, 4, "drums"));
    }

    private void writeAllPersonalInstruments() {
        WritePolicy writePolicy = new WritePolicy();
        // John Lennon
        write(INSTRUMENTS, writePolicy, 1, personalInstrument(1, 1, "vocals"));
        write(INSTRUMENTS, writePolicy, 2, personalInstrument(2, 1, "guitar"));
        write(INSTRUMENTS, writePolicy, 3, personalInstrument(3, 1, "keyboards"));
        write(INSTRUMENTS, writePolicy, 4, personalInstrument(4, 1, "harmonica"));

        // Paul McCartney
        write(INSTRUMENTS, writePolicy, 5, personalInstrument(5, 2, "vocals"));
        write(INSTRUMENTS, writePolicy, 6, personalInstrument(6, 2, "bass guitar"));
        write(INSTRUMENTS, writePolicy, 7, personalInstrument(7, 2, "guitar"));
        write(INSTRUMENTS, writePolicy, 8, personalInstrument(8, 2, "keyboards"));

        // George Harrison
        write(INSTRUMENTS, writePolicy, 9, personalInstrument(9, 3, "vocals"));
        write(INSTRUMENTS, writePolicy, 10, personalInstrument(10, 3, "guitar"));
        write(INSTRUMENTS, writePolicy, 11, personalInstrument(11, 3, "sitar"));

        // Ringo Starr
        write(INSTRUMENTS, writePolicy, 12, personalInstrument(12, 4, "drums"));
        write(INSTRUMENTS, writePolicy, 13, personalInstrument(13, 4, "vocals"));
    }


    //@Test
    void testFunction() {
        writeBeatles();
        Statement statement = new Statement();
        statement.setSetName("people");
        statement.setNamespace("test");
        //statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "add_stat_ops");
        //statement.setAggregateFunction(getClass().getClassLoader(), "sum1.lua", "sum1", "sum_single_bin", new Value.StringValue("year_of_birth"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "sum2.lua", "sum2", "sum_single_bin", new Value.StringValue("year_of_birth"));

        //System.setProperty("lua.dir", "/tmp");
        //statement.setAggregateFunction("sum2", "sum_single_bin", new Value.StringValue("year_of_birth"));

        //statement.setAggregateFunction(getClass().getClassLoader(), "sum3.lua", "sum3", "sum_single_bin", new Value.StringValue("year_of_birth"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "single_bin_stats", new Value.StringValue("year_of_birth"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "single_bin_stats");

        //client.register(new Policy(), getClass().getClassLoader(), "stats.lua", "stats.lua", Language.LUA);
        //client.register(new Policy(), getClass().getClassLoader(), "sum1.lua", "sum1.lua", Language.LUA).waitTillComplete();
        //client.register(new Policy(), getClass().getClassLoader(), "sum2.lua", "sum2.lua", Language.LUA).waitTillComplete();

//        statement.setAggregateFunction(getClass().getClassLoader(), "distinct.lua", "distinct", "distinct", new Value.StringValue("year_of_birth"));
//        client.register(new Policy(), getClass().getClassLoader(), "distinct.lua", "distinct.lua", Language.LUA);


        statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("year_of_birth"), new Value.StringValue("avg:kids_count"));
        client.register(new Policy(), getClass().getClassLoader(), "groupby.lua", "groupby.lua", Language.LUA);


        com.aerospike.client.query.ResultSet rs = client.queryAggregate(null, statement);
        while(rs.next()) {
            System.out.println("rec: " + rs.getObject());
        }
    }

    //@Test
    void select() {
        writeMainPersonalInstruments();
        Statement statement = new Statement();
        statement.setSetName("instruments");
        statement.setNamespace("test");
        //statement.setPredExp(PredExp.integerBin("id"), PredExp.integerValue(2), PredExp.integerEqual());
        //statement.setPredExp(PredExp.integerValue(2), PredExp.integerBin("id"), PredExp.integerEqual());
        statement.setPredExp(PredExp.integerBin("person_id"), PredExp.integerBin("id"), PredExp.integerEqual());
        RecordSet rs = client.query(new QueryPolicy(), statement);
        while (rs.next()) {
            System.out.println(rs.getRecord().bins);
        }
    }
}
