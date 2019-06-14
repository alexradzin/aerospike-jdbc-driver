package com.nosqldriver.aerospike.sql;

import com.nosqldriver.VisibleForPackage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeAllPersonalInstruments;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

/**
 * Tests of SELECT with JOIN
 */
class SelectJoinTest {
    @VisibleForPackage static final Collection<String> guitar  = new HashSet<>(singleton("guitar"));
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
        ResultSet rs = executeQuery(sql, "test", "first_name", "first_name", VARCHAR, "name", "instrument", null /*VARCHAR*/); // FIXME: type of joined columns
        Map<String, Collection<String>> result = collect(rs, "first_name", "instrument");
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
        ResultSet rs = executeQuery(sql, "test", "first_name", "first_name", VARCHAR, "name", "instrument", null /*VARCHAR*/); // FIXME: type of joined columns
        Map<String, Collection<String>> result = collect(rs, "first_name", "instrument");
        assertEquals(1, result.size());
        assertEquals(new HashSet<>(asList("vocals", "guitar", "keyboards", "harmonica")), result.get("John"));
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where instrument='guitar'",
    })
    void oneToManyJoinWhereSecondaryTable(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql, "test", "first_name", "first_name", VARCHAR, "name", "instrument", null /*VARCHAR*/); // FIXME: type of joined columns
        Map<String, Collection<String>> result = collect(rs, "first_name", "instrument");
        assertEquals(3, result.size());
        Arrays.asList("John", "Paul", "George").forEach(name -> assertEquals(guitar, result.get(name)));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='Paul' and i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where p.first_name='Paul' and i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where p.first_name='Paul' and instrument='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='Paul' and instrument='guitar'",
    })
    void oneToManyJoinWhereMainAndSecondaryTable(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql, "test", "first_name", "first_name", VARCHAR, "name", "instrument", null /*VARCHAR*/); // FIXME: type of joined columns
        Map<String, Collection<String>> result = collect(rs, "first_name", "instrument");
        assertEquals(1, result.size());
        assertEquals(guitar, result.get("Paul"));
    }


    @VisibleForPackage static Map<String, Collection<String>> collect(ResultSet rs, String ... names) throws SQLException {
        Map<String, Collection<String>> result = new HashMap<>();
        while(rs.next()) {
            for (int i = 0; i < names.length; i++) {
                assertEquals(rs.getString(i + 1), rs.getString(names[i]));
            }

            String key = rs.getString(1);

            Collection<String> values = result.getOrDefault(key, new HashSet<>());
            for (int i = 1; i < names.length; i++) {
                String value = rs.getString(i + 1);
                if (value != null) {
                    values.add(value);
                }
            }
            result.put(key, values);
        }
        return result;
    }
}
