package com.nosqldriver.aerospike.sql;

import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SELECT_ALL;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.execute;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQueryPreparedStatement;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQueryPreparedStatementWithParameters;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeUpdate;
import static com.nosqldriver.aerospike.sql.TestDataUtils.resultSetNext;
import static com.nosqldriver.aerospike.sql.TestDataUtils.retrieveColumn;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of DELETE SQL statement
 */
class DeleteTest {
    @BeforeEach
    void init() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        writeBeatles();
    }

    @AfterAll
    static void cleanup() {
        deleteAllRecords(NAMESPACE, PEOPLE);
    }


    @Test
    void deleteAll() throws SQLException {
        assertDelete("delete from people", p -> false);
    }

    @Test
    void deleteAllWithNamesapce() throws SQLException {
        assertDelete("delete from test.people", p -> false);
    }

    @Test
    void deleteAllPs() throws SQLException {
        assertDelete("delete from people", new Object[0], p -> false);
    }

    @Test
    void deleteByPkEq() throws SQLException {
        assertDelete("delete from people where PK=1", p -> !"John".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkEqPs() throws SQLException {
        assertDelete("delete from people where PK=?", new Object[] {1}, p -> !"John".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkIn1() throws SQLException {
        assertDelete("delete from people where PK in (1, 2, 3)", p -> "Ringo".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkIn2() throws SQLException {
        assertDelete("delete from people where PK in (2, 3)", p -> "John".equals(p.getFirstName()) || "Ringo".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkBetween() throws SQLException {
        assertDelete("delete from people where PK between 1 and 3", p -> "Ringo".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkBetweenString() throws SQLException {
        assertEquals("BETWEEN cannot be applied to string", assertThrows(SQLException.class, () -> assertDelete("delete from people where PK between '1' and '3'", p -> false)).getMessage());
    }

    @Test
    void deleteByPkBetweenPsBoth() throws SQLException {
        assertDelete("delete from people where PK between ? and ?", new Object[] {1, 3}, p -> "Ringo".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkBetweenPs1() throws SQLException {
        assertDelete("delete from people where PK between ? and 3", new Object[] {1}, p -> "Ringo".equals(p.getFirstName()));
    }

    @Test
    void deleteByPkBetweenPs2() throws SQLException {
        assertDelete("delete from people where PK between 1 and ?", new Object[] {3}, p -> "Ringo".equals(p.getFirstName()));
    }

    @Test
    void deleteByIntCriteria() throws SQLException {
        assertDelete("delete from people where year_of_birth=1940", p -> p.getYearOfBirth() != 1940);
    }

    @Test
    void deleteByIntCriteriaPs() throws SQLException {
        assertDelete("delete from people where year_of_birth=?", new Object[] {1940}, p -> p.getYearOfBirth() != 1940);
    }

    @Test
    void deleteByStringCriteria() throws SQLException {
        assertDelete("delete from people where first_name='John'", p -> !"John".equals(p.getFirstName()));
    }

    @Test
    void deleteByStringCriteriaPs() throws SQLException {
        assertDelete("delete from people where first_name=?", new Object[] {"John"}, p -> !"John".equals(p.getFirstName()));
    }



    private <T> void assertDelete(String deleteSql, Predicate<Person> expectedResultFilter) throws SQLException {
        int expectedUpdatedRowsCount = (int)stream(beatles).filter(person -> !expectedResultFilter.test(person)).count();
        assertDelete(executeUpdate, deleteSql, expectedResultFilter, res -> res == expectedUpdatedRowsCount);
        assertDelete(execute, deleteSql, expectedResultFilter, res -> res);
        assertDelete(executeQuery, deleteSql, expectedResultFilter, rs -> !resultSetNext(rs));
        assertDelete(executeQueryPreparedStatement, deleteSql, expectedResultFilter, rs -> !resultSetNext(rs));

        assertDelete(executeQueryPreparedStatementWithParameters, new Object[] {}, deleteSql, expectedResultFilter, rs -> !resultSetNext(rs));

    }

    private <T> void assertDelete(Function<String, T> executor, String deleteSql, Predicate<Person> expectedResultFilter, Predicate<T> returnValueValidator) throws SQLException {
        writeBeatles();
        Collection<String> names1 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).map(Person::getFirstName).collect(toSet()), names1);

        assertTrue(returnValueValidator.test(executor.apply(deleteSql)));

        Collection<String> names2 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).filter(expectedResultFilter).map(Person::getFirstName).collect(toSet()), names2);
    }


    private <T> void assertDelete(String deleteSql, Object[] parameters, Predicate<Person> expectedResultFilter) throws SQLException {
        assertDelete(executeQueryPreparedStatementWithParameters, parameters, deleteSql, expectedResultFilter, rs -> !resultSetNext(rs));
    }


    private <T> void assertDelete(BiFunction<String, Object[], T> executor, Object[] parameters, String deleteSql, Predicate<Person> expectedResultFilter, Predicate<T> returnValueValidator) throws SQLException {
        writeBeatles();
        Collection<String> names1 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).map(Person::getFirstName).collect(toSet()), names1);

        assertTrue(returnValueValidator.test(executor.apply(deleteSql, parameters)));

        Collection<String> names2 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).filter(expectedResultFilter).map(Person::getFirstName).collect(toSet()), names2);
    }
}
