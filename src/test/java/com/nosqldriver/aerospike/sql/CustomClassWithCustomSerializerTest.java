package com.nosqldriver.aerospike.sql;


import com.nosqldriver.Person;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

import static com.nosqldriver.aerospike.sql.SelectTest.assertGroupByYearOfBirth;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeTestUrl;
import static com.nosqldriver.aerospike.sql.TestDataUtils.beatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

class CustomClassWithCustomSerializerTest  {
    private final Connection testConn;

    CustomClassWithCustomSerializerTest() throws SQLException {
        this.testConn = DriverManager.getConnection(aerospikeTestUrl + "?custom.function.parse=" + PersonParser.class.getName());;
    }


    @BeforeAll
    @AfterAll
    static void dropAll() throws SQLException {
        deleteAllRecords(NAMESPACE, PEOPLE);

        PreparedStatement insert = getTestConnection().prepareStatement("insert into people (PK, data) values (?, ?)");
        for (Person p : beatles) {
            insert.setInt(1, p.getId());
            insert.setObject(2, serialize(p));
            insert.execute();
        }
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select year_of_birth, count(*) from (select person[yearOfBirth] as year_of_birth from (select parse(data) as person from people)) group by year_of_birth",
            "select year_of_birth, count(*) from (select person[yearOfBirth] as year_of_birth from (select parse(data) as person from people)) group by year_of_birth having count(*) > 0",
    })
    void groupByYearOfBirth(String sql) throws SQLException {
        assertGroupByYearOfBirth(testConn, sql);
    }

    private static String serialize(Person p) {
         return String.format("%d,%s,%s,%d,%d", p.getId(), p.getFirstName(), p.getLastName(), p.getYearOfBirth(), p.getKidsCount());
    }

    private static Person deserialize(String str) {
        String[] fields = str.split(",");
        return new Person(Integer.parseInt(fields[0]), fields[1], fields[2], Integer.parseInt(fields[3]), Integer.parseInt(fields[4]));
    }

    public static class PersonParser implements Function<String, Person> {
        @Override
        public Person apply(String s) {
            return deserialize(s);
        }
    }
}
