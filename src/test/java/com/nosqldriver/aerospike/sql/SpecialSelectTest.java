package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.nosqldriver.aerospike.sql.PreparedStatementWithComplexTypesTest.MyNotSerializableClass;
import com.nosqldriver.aerospike.sql.PreparedStatementWithComplexTypesTest.MySerializableClass;
import com.nosqldriver.sql.DataColumn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.TestUtils.getDisplayName;
import static com.nosqldriver.aerospike.sql.TestDataUtils.INSTRUMENTS;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SELECT_ALL;
import static com.nosqldriver.aerospike.sql.TestDataUtils.SUBJECT_SELECTION;
import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeTestUrl;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.executeQuery;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getClient;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static com.nosqldriver.aerospike.sql.TestDataUtils.write;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeData;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeMainPersonalInstruments;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeSubjectSelection;
import static java.lang.String.format;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BLOB;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.OTHER;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

/**
 * Additional tests that verify SELECT SQL statement.
 * This test case includes tests that were not included into {@link SelectTest} because they need special data in DB.
 * Each test implemented here is responsible on filling required data, {@link #dropAll()} method that run after each
 * test must clean all data.
  */
class SpecialSelectTest {
    private final Connection testConn = getTestConnection();

    @BeforeAll
    static void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, INSTRUMENTS);
        deleteAllRecords(NAMESPACE, SUBJECT_SELECTION);
        deleteAllRecords(NAMESPACE, TestDataUtils.DATA);
    }

    @AfterEach
    void dropAllRecords() {
        dropAll();
    }

    @Test
    void selectEmpty() throws SQLException {
        ResultSet rs = testConn.createStatement().executeQuery(SELECT_ALL);
        assertFalse(rs.next());
    }


    @Test
    @DisplayName("select subject, semester, count(*) from subject_selection group by subject, semester")
    void groupByMulti() throws SQLException {
        writeBeatles();
        writeSubjectSelection();
        ResultSet rs = testConn.createStatement().executeQuery(getDisplayName());
        ResultSetMetaData md = rs.getMetaData();
        assertNotNull(md);
        assertEquals(3, md.getColumnCount());
        assertEquals("subject", md.getColumnName(1));
        assertEquals("semester", md.getColumnName(2));
        assertEquals("count(*)", md.getColumnName(3));



        // {ITB001,2={count(*)=2}, MKB114,1={count(*)=2}, ITB001,1={count(*)=3}}
        Map<String, Integer> expected = new HashMap<>();
        expected.put("ITB001,2", 2);
        expected.put("MKB114,1", 2);
        expected.put("ITB001,1", 3);

        Collection<String> groups = new HashSet<>();
        assertTrue(rs.next());
        groups.add(assertSubjectSelection(rs, expected));
        assertTrue(rs.next());
        groups.add(assertSubjectSelection(rs, expected));
        assertTrue(rs.next());
        groups.add(assertSubjectSelection(rs, expected));
        assertEquals(expected.keySet(), groups);

        assertFalse(rs.next());
    }


    private String assertSubjectSelection(ResultSet rs, Map<String, Integer> expected) throws SQLException {
        String subject = rs.getString(1);
        assertEquals(rs.getInt(2), rs.getShort(2));
        assertEquals(rs.getInt(2), rs.getLong(2));
        assertEquals(rs.getInt(2), rs.getByte(2));
        assertEquals(rs.getInt(2), rs.getFloat(2));
        int semester = rs.getInt(2);
        int count = rs.getInt(3);
        String group = subject + "," + semester;
        assertTrue(expected.containsKey(group));
        assertEquals(expected.get(group).intValue(), count);
        return group;
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select first_name, i.name as instrument from people as p join instruments as i on p.id=i.person_id",
            "select first_name, i.name as instrument from people as p inner join instruments as i on p.id=i.person_id",
            "select first_name, i.name as instrument from people as p left join instruments as i on p.id=i.person_id",

            "select first_name, i.name as instrument from instruments as i join people as p on p.id=i.person_id",
            "select first_name, i.name as instrument from instruments as i inner join people as p on p.id=i.person_id",
            "select first_name, i.name as instrument from instruments as i left join people as p on p.id=i.person_id",

            "select first_name, i.name as instrument from instruments as i join people as p on i.person_id=p.PK",
            "select first_name, i.name as instrument from instruments as i inner join people as p on i.person_id=p.PK",
            "select first_name, i.name as instrument from instruments as i left join people as p on i.person_id=p.PK",
    })
    void oneToOneJoin(String sql) throws SQLException {
        writeBeatles();
        writeMainPersonalInstruments();
        ResultSet rs = testConn.createStatement().executeQuery(sql);
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


    @Test
    void stringPK() throws SQLException {
        writeData();
        ResultSet rs = testConn.createStatement().executeQuery("select * from data where PK='one'");
        assertTrue(rs.next());
        assertFalse(rs.next());
        rs.close();
    }

    @Test
    void groupByDouble() throws SQLException {
        WritePolicy writePolicy = new WritePolicy();
        write(writePolicy, new Key(NAMESPACE, TestDataUtils.DATA, "pi"), new Bin("name", "PI"), new Bin("value", 3.14));
        write(writePolicy, new Key(NAMESPACE, TestDataUtils.DATA, "e"), new Bin("name", "EXP"), new Bin("value", 2.7));

        ResultSet rs = testConn.createStatement().executeQuery("select value, count(*) from data group by value");

        Map<Double, Integer> actual = new HashMap<>();
        while(rs.next()) {
            actual.put(rs.getDouble(1), rs.getInt(2));
        }
        assertFalse(rs.next());
        Map<Double, Integer> expected = new HashMap<>();
        expected.put(3.14, 1);
        expected.put(2.7, 1);

        assertEquals(expected, actual);
        rs.close();
    }

    @Test
    void emptyColumnName() throws SQLException {
        double value = Math.PI / 4.0;
        assertFalse(testConn.createStatement().execute(format("insert into data (PK, \"\") values (1, %s)", value)));
        IAerospikeClient client = getClient();
        Key key1 = new Key(NAMESPACE, TestDataUtils.DATA, 1);
        Key key2 = new Key(NAMESPACE, TestDataUtils.DATA, 2);
        client.put(new WritePolicy(), new Key(NAMESPACE, TestDataUtils.DATA, 2), new Bin("", Math.E));

        Map<String,Object> record1 = client.get(new Policy(), key1).bins;
        assertEquals(1, record1.size());
        assertEquals(value, record1.get(""));

        Map<String,Object> record2 = client.get(new Policy(), key2).bins;
        assertEquals(1, record2.size());
        assertEquals(Math.E, record2.get(""));

        Map<Double, Double> actuals = new HashMap<>();
        try(ResultSet rs = executeQuery(testConn, "select \"\", sin(\"\") as sin_empty from data",
                DataColumn.DataColumnRole.DATA.create(NAMESPACE, TestDataUtils.DATA, "", "").withType(DOUBLE),
                DataColumn.DataColumnRole.DATA.create(NAMESPACE, TestDataUtils.DATA, "sin(\"\")", "sin_empty").withType(DOUBLE)
        )) {
            while(rs.next()) {
                double vi1 = rs.getDouble(1);
                double vn1 = rs.getDouble("");
                assertEquals(vi1, vn1);
                double vi2 = rs.getDouble(2);
                actuals.put(vi1, vi2);
            }
        }
        Map<Double, Double> expecteds = Stream.of(value, Math.E).collect(Collectors.toMap(v -> v, Math::sin));
        assertEquals(expecteds, actuals);
    }

    @Test
    void emptyColumnNameMapValue() throws SQLException {
        emptyColumnNameAnyValue(testConn, Collections.singletonMap("hello", "bye"), v->v, "select \"\" as empty from data", "");
    }

    @Test
    void emptyColumnNameSerializableClass() throws SQLException {
        emptyColumnNameAnyValue(testConn, new MySerializableClass(123, "one hundred and twenty four"), v->v, "select \"\" as empty from data", "");
    }

    @Test
    void emptyColumnNameNotSerializableClass() throws SQLException {
        Connection conn = DriverManager.getConnection(aerospikeTestUrl + "?custom.function.deserialize=com.nosqldriver.aerospike.sql.PreparedStatementWithComplexTypesTest$MyCustomDeserializer");
        MyNotSerializableClass obj = new MyNotSerializableClass(456, "two hundred and fifty six");
        emptyColumnNameAnyValue(conn, obj, v -> MyNotSerializableClass.serialize(obj), "select deserialize(\"\") as empty from data", "deserialize(\"\")");
    }

    @Test
    void selectPKSendPk() throws SQLException {
        selectPk(getConnection(aerospikeTestUrl + "?" + "policy.*.sendKey=true"), new Object[] {"one", "two", "three"}, true);
    }

    @Test
    void selectPKNotSendPk() throws SQLException {
        selectPk(testConn, new Object[] {"one", "two", "three"}, false);
    }

    void selectPk(Connection conn, Object[] values, boolean sendPk) throws SQLException {
        writeValues(conn, values);
        int count = 0;
        int pkType = sendPk ? BIGINT : 0;
        String select = "select PK, field from data";
        try(ResultSet rs = executeQuery(conn, select,
                DataColumn.DataColumnRole.PK.create(NAMESPACE, TestDataUtils.DATA, "PK", "PK").withType(pkType),
                DataColumn.DataColumnRole.DATA.create(NAMESPACE, TestDataUtils.DATA, "field", "field").withType(VARCHAR))) {
            while (rs.next()) {
                assertEquals(rs.getObject("PK"), rs.getObject(1));
                if (sendPk) {
                    assertNotNull(rs.getObject("PK"));
                } else {
                    assertNull(rs.getObject("PK"));
                }
                assertEquals(rs.getObject(2), rs.getObject("field"));
                count++;
            }
        }
        assertEquals(values.length, count);
    }

    @Test
    void selectPKAndDigestSendPkAndDigest() throws SQLException {
        selectPkDigest(getConnection(aerospikeTestUrl + "?" + "policy.*.sendKey=true&policy.driver.sendKeyDigest=true"), new Object[] {"one", "two", "three"}, true, true);
    }

    @Test
    void selectPKAndDigestSendPk() throws SQLException {
        selectPkDigest(getConnection(aerospikeTestUrl + "?" + "policy.*.sendKey=true"), new Object[] {"one", "two", "three"}, true, false);
    }

    @Test
    void selectPKAndDigestSendDigest() throws SQLException {
        selectPkDigest(getConnection(aerospikeTestUrl + "?" + "policy.driver.sendKeyDigest=true"), new Object[] {"one", "two", "three"}, false, true);
    }

    void selectPkDigest(Connection conn, Object[] values, boolean sendPk, boolean sendPkDigest) throws SQLException {
        writeValues(conn, values);
        int count = 0;
        int pkType = sendPk ? BIGINT : 0;
        int pkDigestType = sendPkDigest ? BLOB : 0;
        String select = "select PK, PK_DIGEST, field from data";
        try(ResultSet rs = executeQuery(conn, select,
                DataColumn.DataColumnRole.PK.create(NAMESPACE, TestDataUtils.DATA, "PK", "PK").withType(pkType),
                DataColumn.DataColumnRole.PK.create(NAMESPACE, TestDataUtils.DATA, "PK_DIGEST", "PK_DIGEST").withType(pkDigestType),
                DataColumn.DataColumnRole.DATA.create(NAMESPACE, TestDataUtils.DATA, "field", "field").withType(VARCHAR))) {
            while (rs.next()) {
                assertEquals(rs.getObject("PK"), rs.getObject(1));
                if (sendPk) {
                    assertNotNull(rs.getObject("PK"));
                } else {
                    assertNull(rs.getObject("PK"));
                }
                assertEquals(rs.getObject("PK_DIGEST"), rs.getObject(2));
                if (sendPkDigest) {
                    assertNotNull(rs.getObject("PK_DIGEST"));
                } else {
                    assertNull(rs.getObject("PK_DIGEST"));
                }
                assertEquals(rs.getObject(3), rs.getObject("field"));
                count++;
            }
        }
        assertEquals(values.length, count);
    }

    private void writeValues(Connection conn, Object[] values) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("insert into data (PK, field) values (?, ?)");
        int n = values.length;
        for (int i = 0; i < n; i++) {
            ps.setObject(1, i);
            ps.setObject(2, values[i]);
            assertFalse(ps.execute());
        }
    }

    void emptyColumnNameAnyValue(Connection conn, Object value, Function<Object, Object> serializer, String select, String fieldName) throws SQLException {
        PreparedStatement ps = conn.prepareStatement("insert into data (PK, \"\") values (1, ?)");
        ps.setObject(1, serializer.apply(value));
        assertFalse(ps.execute());
        try(ResultSet rs = executeQuery(conn, select, DataColumn.DataColumnRole.DATA.create(NAMESPACE, TestDataUtils.DATA, fieldName, "empty").withType(OTHER))) {
            assertTrue(rs.next());
            assertEquals(value, rs.getObject(1));
            assertFalse(rs.next());
        }
    }

}
