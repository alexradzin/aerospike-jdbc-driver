package com.nosqldriver.aerospike.sql;

import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SELECT_ALL;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.execute;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeUpdate;
import static com.nosqldriver.aerospike.sql.TestDataUtils.resultSetNext;
import static com.nosqldriver.aerospike.sql.TestDataUtils.retrieveColumn;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
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

    private <T> void assertDelete(Function<String, T> executor, String deleteSql, Predicate<Person> expectedResultFilter, Predicate<T> returnValueValidator) throws SQLException {
        writeBeatles();
        Collection<String> names1 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).map(Person::getFirstName).collect(toSet()), names1);

        assertTrue(returnValueValidator.test(executor.apply(deleteSql)));

        Collection<String> names2 = retrieveColumn(SELECT_ALL, "first_name");
        assertEquals(stream(beatles).filter(expectedResultFilter).map(Person::getFirstName).collect(toSet()), names2);
    }
}
