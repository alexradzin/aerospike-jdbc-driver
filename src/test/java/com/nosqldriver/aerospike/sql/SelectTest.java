package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.IndexType;
import com.nosqldriver.Person;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.util.VariableSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.TestUtils.getDisplayName;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SELECT_ALL;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.conn;
import static com.nosqldriver.aerospike.sql.TestDataUtils.createIndex;
import static com.nosqldriver.aerospike.sql.TestDataUtils.dropIndexSafely;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQueryPreparedStatement;
import static com.nosqldriver.aerospike.sql.TestDataUtils.toListOfMaps;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.lang.String.format;
import static java.sql.Types.BIGINT;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

/**
 * Tests of SELECT SQL statement.
 * All tests in this class use the same pre-requisites: PEOPLE table filled by 4 members of The Beatles.
 * For performance reasons the data is filled in the beginning of the test case.
 */
class SelectTest {
    @AfterEach
    void clean() {
        dropIndexSafely("first_name");
        dropIndexSafely("year_of_birth");
    }


    @BeforeAll
    static void init() {
        TestDataUtils.writeBeatles();
    }

    @AfterAll
    static void dropAll() {
        TestDataUtils.deleteAllRecords(NAMESPACE, PEOPLE);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            SELECT_ALL,
            "select * from people as p",
            "select * from people where 0=0",
            "select * from people where 0=0 and 1=1",
            "select * from people where 0<1 or 2>1",
            "select * from (select * from people)",
            "select * from (select * from people where 0=0)",
            "select * from (select * from people) where 1=1",
            "select * from (select * from people where 0=0) where 1=1",
    })
    void selectAll(String sql) throws SQLException {
        selectAll(sql, executeQuery);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {SELECT_ALL, "select * from people as p"})
    void selectAllWithPreparedStatement(String sql) throws SQLException {
        selectAll(sql, executeQueryPreparedStatement);
    }


    private void selectAll(String sql, Function<String, ResultSet> executor) throws SQLException {
        ResultSet rs = executor.apply(sql);

        ResultSetMetaData md = rs.getMetaData();
        assertEquals(NAMESPACE, md.getSchemaName(1));

        int nColumns = md.getColumnCount();
        assertEquals(5, nColumns);
        Map<String, Integer> actualTypes = new HashMap<>();
        List<String> columnNames = new ArrayList<>();
        List<String> columnLabels = new ArrayList<>();
        for (int i = 0; i < nColumns; i++) {
            String name = md.getColumnName(i + 1);
            String label = md.getColumnLabel(i + 1);
            columnNames.add(name);
            columnLabels.add(label);
            actualTypes.put(name, md.getColumnType(i +1));
        }
        assertEquals(columnNames, columnLabels);
        Map<String, Integer> expectedTypes = new HashMap<>();
        expectedTypes.put("first_name", VARCHAR);
        expectedTypes.put("last_name", VARCHAR);
        expectedTypes.put("id", BIGINT);
        expectedTypes.put("year_of_birth", BIGINT);
        expectedTypes.put("kids_count", BIGINT);
        assertEquals(expectedTypes, actualTypes);


        Map<Integer, String> selectedPeople = new HashMap<>();
        while (rs.next()) {
            for (int i = 0; i < nColumns; i++) {
                String name = columnNames.get(i);
                switch (actualTypes.get(name)) {
                    case VARCHAR: assertEquals(rs.getString(i + 1), rs.getString(name)); break;
                    case BIGINT: assertEquals(rs.getInt(i + 1), rs.getInt(name)); break;
                    default: throw new IllegalArgumentException("Unexpected column type " + actualTypes.get(name));
                }
            }
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
            "select * from (select first_name, year_of_birth from people)",
            "select first_name, year_of_birth from (select * from people)",
            "select first_name, year_of_birth from (select first_name, last_name, year_of_birth from people)",
    })
    void selectSpecificFields(String sql) throws SQLException {
        selectSpecificFields(sql, sql1 -> {
            try {
                return conn.createStatement().executeQuery(sql);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
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
    void selectSpecificFieldsUsingExecute(String sql) throws SQLException {
        selectSpecificFields(sql, sql1 -> {
            try {
                java.sql.Statement statement = conn.createStatement();
                statement.execute(sql1);
                return statement.getResultSet();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @VisibleForPackage // visible for tests
    private static Stream<Arguments> orderBy = Stream.of(
            Arguments.of("select * from people order by first_name", new String[] {"George", "John", "Paul", "Ringo"}),
            Arguments.of("select * from people order by first_name desc", new String[] {"Ringo", "Paul", "John", "George"}),
            Arguments.of("select * from people order by year_of_birth, kids_count", new String[] {"John", "Ringo", "Paul", "George"}),
            Arguments.of("select * from people order by year_of_birth, kids_count desc", new String[] {"Ringo", "John", "Paul", "George"}),
            Arguments.of("select * from people order by first_name limit 3", new String[] {"George", "John", "Paul"}),
            Arguments.of("select * from people order by first_name offset 1", new String[] {"John", "Paul", "Ringo"}),
            Arguments.of("select * from people order by first_name limit 2 offset 1", new String[] {"John", "Paul"})
    );
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @VariableSource("orderBy")
    void selectWithOrderBy(String query, String[] expected) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertArrayEquals(expected, toListOfMaps(rs).stream().map(e -> (String)e.get("first_name")).toArray(String[]::new));
    }



    private void selectSpecificFields(String sql, Function<String, ResultSet> resultSetFactory) throws SQLException {
        ResultSet rs = resultSetFactory.apply(sql);
        Map<String, Integer> selectedPeople = new HashMap<>();


        ResultSetMetaData metaData = rs.getMetaData();
        assertEquals(2, metaData.getColumnCount());
        assertEquals(NAMESPACE, metaData.getSchemaName(1));

        assertEquals("first_name", metaData.getColumnName(1));
        //assertEquals("people", metaData.getTableName(1));
        assertEquals(VARCHAR, metaData.getColumnType(1));
        assertEquals("varchar", metaData.getColumnTypeName(1));

        assertEquals("year_of_birth", metaData.getColumnName(2));
        //assertEquals("people", metaData.getTableName(2));
        assertEquals(VARCHAR, metaData.getColumnType(1));
        assertEquals("varchar", metaData.getColumnTypeName(1));


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
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());

        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount());
        //assertEquals("1", rs.getMetaData().getColumnName(1));
        assertEquals("one", md.getColumnLabel(1));
        assertEquals(INTEGER, md.getColumnType(1));


        assertTrue(rs.next());
        assertEquals("one", rs.getMetaData().getColumnLabel(1));
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("one"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("select 1 as one from people where PK=?")
    void select1fromPeopleUsingPreparedStatement() throws SQLException {
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
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("nine", md.getColumnLabel(1));
        assertEquals(INTEGER, md.getColumnType(1));

        assertTrue(rs.next());
        assertEquals("nine", rs.getMetaData().getColumnLabel(1));
        assertEquals(9, rs.getInt(1));
        assertEquals(9, rs.getInt("nine"));
        assertFalse(rs.next());
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select 1 as number union all select 2 as number",
            "select 1 as number union select 2 as number",
    })
    void selectIntUnion(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("number", md.getColumnLabel(1));
        assertEquals(INTEGER, md.getColumnType(1));

        assertTrue(rs.next());
        assertEquals("number", rs.getMetaData().getColumnLabel(1));
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("number"));
        assertTrue(rs.next());
        assertEquals("number", rs.getMetaData().getColumnLabel(1));
        assertEquals(2, rs.getInt(1));
        assertEquals(2, rs.getInt("number"));

        assertFalse(rs.next());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select 1 as number union select 1 as number",
    })
    void selectIntUnionWithRepeatedValues(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("number", md.getColumnLabel(1));
        assertEquals(INTEGER, md.getColumnType(1));

        assertTrue(rs.next());
        assertEquals("number", rs.getMetaData().getColumnLabel(1));
        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("number"));
        assertFalse(rs.next());
    }



    @Test
    @DisplayName("select (4+5)/3 as three, first_name as name, year_of_birth - 1900 as year from people where PK=1")
    void selectExpressionAndFieldFromPeople() throws SQLException {
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
        ResultSet rs = conn.createStatement().executeQuery("select 1 as one, 2 + 3 as two_plus_three, (1+2)*3 as someexpr, year() - year_of_birth as years_ago, first_name as name, year_of_birth - 1900 as y, len(last_name) as surname_length from people");

        Map<String, Integer> selectedPeople = new HashMap<>();

        ResultSetMetaData md = rs.getMetaData();
        String[] expectedLabels = new String[] {"one", "two_plus_three", "someexpr", "years_ago", "name", "y", "surname_length"};
        for (int i = 0; i < expectedLabels.length; i++) {
            assertEquals(expectedLabels[i], md.getColumnLabel(i + 1));
        }
        String[] expectedNames = new String[] {"1", "2 + 3", "(1 + 2) * 3", "year() - year_of_birth", "first_name", "year_of_birth - 1900", "len(last_name)"};
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
        for (int i = 0; i < beatles.length; i++) {
            int id = i + 1;
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where PK=%d", id));

            assertTrue(rs.next());
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
            assertEquals(beatles[i].getId(), rs.getInt("id"));
            assertEquals(beatles[i].getFirstName(), rs.getString("first_name"));
            assertEquals(beatles[i].getLastName(), rs.getString("last_name"));
            assertEquals(beatles[i].getYearOfBirth(), rs.getInt("year_of_birth"));
        }
    }

    @Test
    void selectByNotIndexedField() throws SQLException {
        for (Person person : beatles) {
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where last_name=%s", person.getLastName()));

            assertTrue(rs.next());
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
            assertEquals(person.getId(), rs.getInt("id"));
            assertEquals(person.getFirstName(), rs.getString("first_name"));
            assertEquals(person.getLastName(), rs.getString("last_name"));
            assertEquals(person.getYearOfBirth(), rs.getInt("year_of_birth"));
        }
    }

    @Test
    void selectByOneStringIndexedField() throws SQLException {
        createIndex("first_name", IndexType.STRING);
        for (Person person : beatles) {
            ResultSet rs = conn.createStatement().executeQuery(format("select * from people where first_name=%s", person.getFirstName()));

            assertTrue(rs.next());
            assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
            assertEquals(person.getId(), rs.getInt("id"));
            assertEquals(person.getFirstName(), rs.getString("first_name"));
            assertEquals(person.getLastName(), rs.getString("last_name"));
            assertEquals(person.getYearOfBirth(), rs.getInt("year_of_birth"));
        }
    }

    @Test
    @DisplayName("year_of_birth=1942 -> [Paul], year_of_birth=1943 -> [George]")
    void selectOneRecordByOneNumericIndexedFieldEq() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField("=", 1942, 2);
        assertSelectByOneNumericIndexedField("=", 1943, 3);
    }

    @Test
    @DisplayName("year_of_birth=1940 -> [John, Ringo]")
    void selectSeveralRecordsByOneNumericIndexedFieldEq() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField("=", 1940, 1, 4);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select * from people where year_of_birth-1900=40",
            "select * from people where year_of_birth-1900=2*20",
            "select * from people where year_of_birth-1900=2*2*10",
            "select * from people where year_of_birth-19*100=2*2*10",
            "select * from people where year_of_birth-19*(20+80)=(2+3-1)*100/10",
    })
    void selectSeveralRecordsByOneFieldWithCalculationInWhereClause(String sql) throws SQLException {
        assertSelect(sql, 1, 4);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select * from people where year_of_birth=1940 and first_name='John'",
            "select * from people where year_of_birth=1940 and first_name='John' and 'a'='a'",
            "select * from people where year_of_birth=1940 and 1>0 and first_name='John'",
            "select * from people where 2>=2 and year_of_birth=1940 and first_name='John'",
            "select * from people where 2>=2 and year_of_birth=1940 and 0<=0 and first_name='John' and 2<3"
    })
    void selectOneRecordByOneNumericIndexedFieldEqAndOneNotIndexedField(String sql) throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect(sql, 1);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select * from people where year_of_birth=1940 and first_name='John' and 'a'='b'",
            "select * from (select * from people where year_of_birth=1940 and first_name='John') where 'a'='b'",
            "select * from (select * from people where year_of_birth=1940 and first_name='John') where 1=0",
            "select * from (select * from people) where 1=0",
    })
    void selectRecordsWithFalseEvaluatedWhereExpression(String sql) throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect(sql);
    }


    @Test
    @DisplayName("year_of_birth=1940 and first_name='John'-> [John]")
    void selectOneRecordByOneNumericIndexedFieldEqAndOneNotIndexedFieldUsingPreparedStatement() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect("select * from people where year_of_birth=1940 and first_name='John'", 1);

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
        assertSelect("select * from people where year_of_birth=1940 and last_name='Lennon'", 1);
    }


    @Test
    @DisplayName("last_name='Lennon' or last_name='Harrison' -> [John, George]")
    void selectSeveralPersonsByLastNameOr() throws SQLException {
        assertSelect("select * from people where last_name='Lennon' or last_name='Harrison'", 1, 3);
    }


    @Test
    @DisplayName("year_of_birth=1939 -> nothing")
    void selectNothingByOneNumericIndexedFieldEq() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField("=", 1939);
    }

    @Test
    @DisplayName("year_of_birth>1939 -> [John, Paul, George, Ringo]")
    void selectAllRecordsByOneNumericIndexedFieldGt() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(">", 1939, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("year_of_birth>=1940 -> [John, Paul, George, Ringo]")
    void selectAllRecordsByOneNumericIndexedFieldGe() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(">=", 1940, 1, 2, 3, 4);
    }

    @Test
    @DisplayName("year_of_birth>1940 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldGe() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField(">", 1940, 2, 3);
    }

    @Test
    @DisplayName("year_of_birth between 1942 and 1943 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldBetween() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect("select * from people where year_of_birth between 1942 and 1943", 2, 3);
    }

    @Test
    @DisplayName("year_of_birth between 1941 and 1944 -> [Paul, George]")
    void selectSeveralRecordsByOneNumericIndexedFieldBetween2() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelect("select * from people where year_of_birth between 1941 and 1944", 2, 3);
    }


    @Test
    @DisplayName("PK IN (2, 3) -> [Paul, George]")
    void selectSeveralRecordsByPkIn() throws SQLException {
        assertSelect("select * from people where PK in (2, 3)", 2, 3);
    }

    @Test
    @DisplayName("PK IN (20, 30) -> []")
    void selectNoRecordsByPkInWrongIds() throws SQLException {
        assertSelect("select * from people where PK in (20, 30)");
    }

    @Test
    @DisplayName("id IN (2, 3) -> [Paul, George]")
    void selectSeveralRecordsByIntColumnIn() throws SQLException {
        assertSelect("select * from people where id in (2, 3)", 2, 3);
    }

    @Test
    @DisplayName("id IN (1) -> [John]")
    void selectOneRecordByIntColumnIn() throws SQLException {
        assertSelect("select * from people where id in (1)", 1);
    }

    @Test
    @DisplayName("id IN (1, 2, 3, 4) -> [John, Paul, George, Ringo]")
    void selectAllRecordsByIntColumnIn() throws SQLException {
        assertSelect("select * from people where id in (1, 2, 3, 4)", 1, 2, 3, 4);
    }

    @Test
    @DisplayName("id IN (22, 33) -> []")
    void selectNoRecordsByIntColumnIn() throws SQLException {
        assertSelect("select * from people where id in (22, 33)");
    }

    @Test
    @DisplayName("first_name IN ('Paul', 'George') -> [Paul, George]")
    void selectSeveralRecordsByStringColumnIn() throws SQLException {
        assertSelect("select * from people where first_name in ('Paul', 'George')", 2, 3);
    }


    @Test
    @DisplayName("year_of_birth<1939 -> nothing")
    void selectNothingRecordsByOneNumericIndexedFieldLt() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField("<", 1939);
    }

    @Test
    @DisplayName("year_of_birth<1940 -> nothing")
    void selectNothingRecordsByOneNumericIndexedFieldLt2() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField("<", 1940);
    }

    @Test
    @DisplayName("year_of_birth<=1940 -> [John, Ringo]")
    void selectNothingRecordsByOneNumericIndexedFieldLe() throws SQLException {
        createIndex("year_of_birth", IndexType.NUMERIC);
        assertSelectByOneNumericIndexedField("<=", 1940, 1, 4);
    }

    @Test
    @DisplayName("limit 2 -> [John, Paul]")
    void selectAllWithLimit2() throws SQLException {
        assertSelect("select * from people limit 2", 1, 2);

        //ResultSet rs = conn.createStatement().executeQuery("select * from people limit 2");
        // "id", "first_name", "last_name", "year_of_birth", "kids_count"
        ResultSet rs = executeQuery("select * from people limit 2", NAMESPACE, false, "id", null, INTEGER, "first_name", null, VARCHAR, "last_name", null, VARCHAR, "year_of_birth", null, INTEGER, "kids_count", null, INTEGER);
        //assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        int n = 0;
        //noinspection StatementWithEmptyBody // counter
        for (; rs.next(); n++);
        assertEquals(2, n);
    }

    @Test
    @DisplayName("offset 1 -> [Paul, George, Ringo]")
    void selectAllWithOffset1() throws SQLException {
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
        ResultSet rs = executeQuery(getDisplayName(), NAMESPACE, true,
                "count(*)", "n", BIGINT,
                "min(year_of_birth)", "min", BIGINT,
                "max(year_of_birth)", "max", BIGINT,
                "avg(year_of_birth)", "avg", DOUBLE,
                "sum(year_of_birth)", "total", BIGINT
        );


        assertTrue(rs.next());

        assertEquals(4, rs.getInt(1));
        assertEquals(4, rs.getInt("n"));
        assertEquals(1940, rs.getInt(2));
        assertEquals(1940, rs.getInt("min"));
        assertEquals(1943, rs.getInt(3));
        assertEquals(1943, rs.getInt("max"));
        double average = stream(beatles).mapToInt(Person::getYearOfBirth).average().orElseThrow(() -> new IllegalStateException("No average found"));
        assertEquals(average, rs.getDouble(4), 0.001);
        assertEquals(average, rs.getDouble("avg"), 0.001);
        int sum = stream(beatles).mapToInt(Person::getYearOfBirth).sum();
        assertEquals(sum, rs.getInt(5));
        assertEquals(sum, rs.getInt("total"));
        assertFalse(rs.next());
    }

    @Test
    @DisplayName("select distinct(year_of_birth) as year from people")
    void selectDistinctYearOfBirth() throws SQLException {
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
        assertEquals(new HashSet<>(asList(1940, 1942, 1943)), years);
    }

    @Test
    @DisplayName("select distinct(first_name) as given_name from people")
    void selectDistinctFirstName() throws SQLException {
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
        assertEquals(stream(beatles).map(Person::getFirstName).collect(Collectors.toSet()), names);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year_of_birth, count(*) from people group by year_of_birth",
            "select year_of_birth, count(*) from people group by year_of_birth having count(*) > 0",
    })
    void groupByYearOfBirth(String sql) throws SQLException {
        assertGroupByYearOfBirth(sql);
    }

    @Test
    @DisplayName("select year_of_birth as yob, count(*) as n from people group by year_of_birth")
    void groupByYearOfBirthWithAliases() throws SQLException {
        ResultSetMetaData md = assertGroupByYearOfBirth(getDisplayName());
        assertEquals("yob", md.getColumnLabel(1));
        assertEquals("n", md.getColumnLabel(2));
    }


    private ResultSetMetaData assertGroupByYearOfBirth(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("year_of_birth", md.getColumnName(1));
        assertEquals(INTEGER, md.getColumnType(1));
        assertEquals("count(*)", md.getColumnName(2));
        assertEquals(BIGINT, md.getColumnType(2));

        Collection<Integer> years = new HashSet<>();
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertEquals(stream(beatles).map(Person::getYearOfBirth).collect(Collectors.toSet()), years);

        return md;
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year_of_birth, count(*) from people group by year_of_birth having count(*) = 2",
            "select year_of_birth, count(*) as num from people group by year_of_birth having count(*) = 2",
            "select year_of_birth, count(*) as n from people group by year_of_birth having n = 2",
            "select year_of_birth, count(*) as n from people group by year_of_birth having n = 4 - 2",
            "select year_of_birth, count(*) from people group by year_of_birth having count(*) > 1",
            "select year_of_birth, count(*) as n from people group by year_of_birth having n > 1"
    })
    void groupByYearOfBirthHavingCount2(String sql) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(2, md.getColumnCount());
        assertEquals("year_of_birth", md.getColumnName(1));
        assertEquals(INTEGER, md.getColumnType(1));
        assertEquals("count(*)", md.getColumnName(2));
        assertEquals(BIGINT, md.getColumnType(2));

        Collection<Integer> years = new HashSet<>();
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertFalse(rs.next());
        assertEquals(Collections.singleton(1940), years);
    }


    @Test
    void groupByYearOfBirthWithoutRequestingMetadata() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery("select year_of_birth, count(*) from people group by year_of_birth");
        Collection<Integer> years = new HashSet<>();
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertTrue(rs.next());
        years.add(assertCounts(rs));
        assertEquals(stream(beatles).map(Person::getYearOfBirth).collect(Collectors.toSet()), years);
    }

    @Test
    @DisplayName("select year_of_birth as year, sum(kids_count) as total_kids from people group by year_of_birth")
    void groupByYearOfBirthWithAggregation() throws SQLException {
        ResultSet rs = executeQuery(getDisplayName(), NAMESPACE, true, "year_of_birth", "year", null /*INTEGER*/, "sum(kids_count)", "total_kids", null /*INTEGER*/); // FIXME: types

        Collection<Integer> years = new HashSet<>();
        assertTrue(rs.next());
        years.add(assertSums(rs));
        assertTrue(rs.next());
        years.add(assertSums(rs));
        assertTrue(rs.next());
        years.add(assertSums(rs));
        assertEquals(stream(beatles).map(Person::getYearOfBirth).collect(Collectors.toSet()), years);

        assertFalse(rs.next());
    }


    @Test
    @DisplayName("select year_of_birth, count(*) from people group by year_of_birth order by count(*) desc, year_of_birth")
    void groupByOrderByCountDesc() throws SQLException {
        groupByOrderBy(getDisplayName(), "year_of_birth", new Object[] {1940, 1942, 1943});
    }

    @Test
    @DisplayName("select year_of_birth, count(*) from people group by year_of_birth order by count(*) desc, year_of_birth")
    void groupByOrderByCountDescCount() throws SQLException {
        groupByOrderBy(getDisplayName(), "count(*)", new Object[] {2L, 1L, 1L});
    }

    @Test
    @DisplayName("select year_of_birth, count(*) from people group by year_of_birth order by count(*), year_of_birth")
    void groupByOrderByCount() throws SQLException {
        groupByOrderBy(getDisplayName(), "year_of_birth", new Object[] {1942, 1943, 1940});
    }

    private void groupByOrderBy(String query, String column, Object[] expected) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(query);
        assertArrayEquals(expected, toListOfMaps(rs).stream().map(e -> e.get(column)).toArray());
    }



    @Test
    @DisplayName("select year_of_birth as year, count(*) as counter, sum(kids_count) as total_kids, max(kids_count) as max_kids, min(kids_count) as min_kids from people group by year_of_birth")
    void groupByYearOfBirthWithMultipleAggregations() throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(5, md.getColumnCount());
        assertEquals("year_of_birth", md.getColumnName(1));

        assertEquals("count(*)", md.getColumnName(2));
        assertEquals("counter", md.getColumnLabel(2));

        assertEquals("sum(kids_count)", md.getColumnName(3));
        assertEquals("total_kids", md.getColumnLabel(3));

        assertEquals("max(kids_count)", md.getColumnName(4));
        assertEquals("max_kids", md.getColumnLabel(4));

        assertEquals("min(kids_count)", md.getColumnName(5));
        assertEquals("min_kids", md.getColumnLabel(5));


        Collection<Integer> years = new HashSet<>();
        assertTrue(rs.next());
        years.add(assertStats(rs));
        assertTrue(rs.next());
        years.add(assertStats(rs));
        assertTrue(rs.next());
        years.add(assertStats(rs));
        assertEquals(stream(beatles).map(Person::getYearOfBirth).collect(Collectors.toSet()), years);

        assertFalse(rs.next());
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, last_name from people where year_of_birth=1940 union all select first_name, last_name from people where year_of_birth>1940",
            "select first_name, last_name from people where year_of_birth=1940 union select first_name, last_name from people where year_of_birth>1940",
            "select first_name, last_name from people union select first_name, last_name from people",
    })
    void unionAll(String query) throws SQLException {
        ResultSet rs = executeQuery(query, DATA.create(NAMESPACE, PEOPLE, "first_name", "first_name").withType(VARCHAR), DATA.create(NAMESPACE, PEOPLE, "last_name", "last_name").withType(VARCHAR));

        List<String> selectedPeople = new ArrayList<>();

        while (rs.next()) {
            selectedPeople.add(rs.getString("first_name"));
        }

        assertFalse(rs.next());
        assertEquals(new HashSet<>(asList("John", "Ringo", "Paul", "George")), new HashSet<>(selectedPeople));
    }



    private int assertCounts(ResultSet rs) throws SQLException {
        int yearOfBirth = rs.getInt(1);
        assertEquals(count(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(2), 0.01);
        return yearOfBirth;
    }

    private int assertSums(ResultSet rs) throws SQLException {
        int yearOfBirth = rs.getInt(1);
        assertEquals(sum(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(2), 0.01);
        return yearOfBirth;
    }

    private int assertStats(ResultSet rs) throws SQLException {
        int yearOfBirth = rs.getInt(1);
        assertEquals(count(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(2), 0.01);
        assertEquals(sum(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(3), 0.01);
        assertEquals(max(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(4), 0.01);
        assertEquals(min(stream(beatles), p -> p.getYearOfBirth() == yearOfBirth, Person::getKidsCount), rs.getDouble(5), 0.01);
        return yearOfBirth;
    }

    private <T> long count(Stream<T> stream, Predicate<T> filter, ToIntFunction<T> pf) {
        return stream.filter(filter).mapToInt(pf).count();
    }

    private <T> int sum(Stream<T> stream, Predicate<T> filter, ToIntFunction<T> pf) {
        return stream.filter(filter).mapToInt(pf).sum();
    }

    private <T> int max(Stream<T> stream, Predicate<T> filter, ToIntFunction<T> pf) {
        return stream.filter(filter).mapToInt(pf).max().orElseThrow(() -> new IllegalStateException("Cannot calculate maximum"));
    }

    private <T> int min(Stream<T> stream, Predicate<T> filter, ToIntFunction<T> pf) {
        return stream.filter(filter).mapToInt(pf).min().orElseThrow(() -> new IllegalStateException("Cannot calculate minumum"));
    }

    private void assertAggregateOneField(String sql, String name, String label, int expected) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(sql);
        assertEquals(NAMESPACE, rs.getMetaData().getSchemaName(1));
        ResultSetMetaData md = rs.getMetaData();
        assertEquals(1, md.getColumnCount());
        assertEquals(name, md.getColumnName(1));
        assertEquals(label, md.getColumnLabel(1));
        assertEquals(BIGINT, md.getColumnType(1));
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
        assertEquals(expected, rs.getInt(label));
        assertFalse(rs.next());
    }

    private void assertSelectByOneNumericIndexedField(String operation, int year, int ... expectedIds) throws SQLException {
        assertSelect(format("select * from people where year_of_birth%s%s", operation, year), expectedIds);
    }

    private void assertSelect(String sql, int ... expectedIds) throws SQLException {
        ResultSet rs = executeQuery(sql,
                DATA.create(NAMESPACE, PEOPLE, "kids_count", "kids_count").withType(BIGINT),
                DATA.create(NAMESPACE, PEOPLE, "last_name", "last_name").withType(VARCHAR),
                DATA.create(NAMESPACE, PEOPLE, "id", "id").withType(BIGINT),
                DATA.create(NAMESPACE, PEOPLE, "first_name", "first_name").withType(VARCHAR),
                DATA.create(NAMESPACE, PEOPLE, "year_of_birth", "year_of_birth").withType(BIGINT)
        );
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
}
