package com.nosqldriver.aerospike.sql;

import com.nosqldriver.VisibleForPackage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.assertFindColumn;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeAllPersonalInstruments;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.sql.Types.BIGINT;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
            "select first_name, i.name as instrument from test.people as p join test.instruments as i on p.id=i.person_id",

            "select first_name, i.name as instrument from people as p join (select * from instruments) as i on p.id=i.person_id",
    })
    void oneToManyJoin(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql, NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "instrument", VARCHAR);
        assertFindColumn(rs, "first_name", "instrument");
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "instrument");
        assertEquals(4, result.size());
        assertEquals(new HashSet<>(asList("vocals", "guitar", "keyboards", "harmonica")), result.get("John"));
        assertEquals(new HashSet<>(asList("vocals", "bass guitar", "guitar", "keyboards")), result.get("Paul"));
        assertEquals(new HashSet<>(asList("vocals", "guitar", "sitar")), result.get("George"));
        assertEquals(new HashSet<>(asList("vocals", "drums")), result.get("Ringo"));
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select * from people as p join instruments as i on p.id=i.person_id",
    })
    @Disabled //FIXME: fails on build server
    void oneToManyJoinSelectAllFields(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql,
                DATA.create(NAMESPACE, PEOPLE, "kids_count", "kids_count").withType(BIGINT),
                DATA.create(NAMESPACE, PEOPLE, "last_name", "last_name").withType(VARCHAR),
                DATA.create(NAMESPACE, PEOPLE, "id", "id").withType(BIGINT),
                DATA.create(NAMESPACE, PEOPLE, "first_name", "first_name").withType(VARCHAR),
                DATA.create(NAMESPACE, PEOPLE, "year_of_birth", "year_of_birth").withType(BIGINT),
                DATA.create(NAMESPACE, INSTRUMENTS, "name", "name").withType(VARCHAR),
                DATA.create(NAMESPACE, INSTRUMENTS, "id", "id").withType(BIGINT),
                DATA.create(NAMESPACE, INSTRUMENTS, "person_id", "person_id").withType(BIGINT)
        );


        assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, rs.getHoldability());
        assertFalse(rs.isClosed());
        assertFindColumn(rs, "kids_count", "last_name", "id", "first_name", "year_of_birth", "name", null /*id from second table; the label is the same as first one, so it finds the first*/, "person_id");
        Map<String, Collection<String>> result = collect(rs, 0, "first_name", "name");
        assertEquals(4, result.size());
        assertEquals(new HashSet<>(asList("vocals", "guitar", "keyboards", "harmonica")), result.get("John"));
        assertEquals(new HashSet<>(asList("vocals", "bass guitar", "guitar", "keyboards")), result.get("Paul"));
        assertEquals(new HashSet<>(asList("vocals", "guitar", "sitar")), result.get("George"));
        assertEquals(new HashSet<>(asList("vocals", "drums")), result.get("Ringo"));


        assertFalse(rs.isClosed());
        rs.close();
        assertTrue(rs.isClosed());
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='John'",
    })
    void oneToManyJoinWhereMainTable(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql, NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "instrument", VARCHAR);
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "instrument");
        assertEquals(1, result.size());
        assertEquals(new HashSet<>(asList("vocals", "guitar", "keyboards", "harmonica")), result.get("John"));
        assertFindColumn(rs, "first_name", "instrument");
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where instrument='guitar'",
    })
    void oneToManyJoinWhereSecondaryTable(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql, NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "instrument", VARCHAR);
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "instrument");
        assertEquals(3, result.size());
        asList("John", "Paul", "George").forEach(name -> assertEquals(guitar, result.get(name)));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='Paul' and i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where p.first_name='Paul' and i.name='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where p.first_name='Paul' and instrument='guitar'",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id where first_name='Paul' and instrument='guitar'",
    })
    void oneToManyJoinWhereMainAndSecondaryTable(String sql) throws SQLException {
        ResultSet rs = executeQuery(sql, NAMESPACE, true, "first_name", "first_name", VARCHAR, "name", "instrument", VARCHAR);
        Map<String, Collection<String>> result = collect(rs, 1, "first_name", "instrument");
        assertEquals(1, result.size());
        assertEquals(guitar, result.get("Paul"));
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id!=i.person_id",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id>i.person_id",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id>=i.person_id",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id<i.person_id",
            "select first_name, i.name as instrument from people as p join instruments as i on p.id<=i.person_id",
    })
    void wrongJoin(String sql) {
        Connection testConn = getTestConnection();
        assertTrue(assertThrows(SQLException.class, () -> testConn.createStatement().executeQuery(sql)).getMessage().startsWith("Join condition must use = only but was"));
    }


    @VisibleForPackage static Map<String, Collection<String>> collect(ResultSet rs, int keyIndex, String keyName, String ... valueNames) throws SQLException {
        Map<String, Collection<String>> result = new HashMap<>();
        if (keyIndex > 0) {
            assertEquals(keyIndex, rs.findColumn(keyName));
        }
        while(rs.next()) {
            if (keyIndex > 0) {
                assertEquals(keyIndex, rs.findColumn(keyName));
            }
            assertThrows(SQLException.class, () -> rs.findColumn("does_not_exist"));
            String key = rs.getString(keyName);
            Collection<String> values = result.getOrDefault(key, new HashSet<>());
            for (String valueName : valueNames) {
                String value = rs.getString(valueName);
                if (value != null) {
                    values.add(value);
                }
            }
            result.put(key, values);
        }
        return result;
    }
}
