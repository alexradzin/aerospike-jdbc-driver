package com.nosqldriver.aerospike.sql;


import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static com.nosqldriver.aerospike.sql.SelectTest.assertGroupByYearOfBirth;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

class SerializableCustomClassTest {
    private static final Connection testConn = getTestConnection();


    @BeforeAll
    static void init() throws SQLException {
        dropAll();

        PreparedStatement insert = getTestConnection().prepareStatement("insert into people (PK, data) values (?, ?)");
        for (Person p : beatles) {
            insert.setInt(1, p.getId());
            insert.setObject(2, p);
            insert.execute();
        }
    }

    @AfterAll
    static void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year_of_birth, count(*) from (select data[yearOfBirth] as year_of_birth from (select data as person from people)) group by year_of_birth",
            "select year_of_birth, count(*) from (select data[yearOfBirth] as year_of_birth from (select data as person from people)) group by year_of_birth having count(*) > 0",
    })
    void groupByYearOfBirth(String sql) throws SQLException {
        assertGroupByYearOfBirth(testConn, sql);
    }


}
