package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.client;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.testConn;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArrayTest {
    private static final String DATA = "data";

    @BeforeEach
    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, PEOPLE);
        deleteAllRecords(NAMESPACE, DATA);
    }

    @Test
    void insertOneRowUsingPreparedStatement() throws SQLException {
        PreparedStatement insert = testConn.prepareStatement("insert into people (PK, id, first_name, last_name, kids) values (?, ?, ?, ?, ?)");
        insert.setInt(1, 1);
        insert.setInt(2, 1);
        insert.setString(3, "John");
        insert.setString(4, "Lennon");
        insert.setArray(5, testConn.createArrayOf("varchar", new String[] {"Sean", "Julian"}));
        int rowCount = insert.executeUpdate();
        assertEquals(1, rowCount);
        Record record = client.get(null, new Key("test", "people", 1));
        assertNotNull(record);
        Map<String, Object> expectedData = new HashMap<>();
        expectedData.put("id", 1L);
        expectedData.put("first_name", "John");
        expectedData.put("last_name", "Lennon");
        expectedData.put("kids", Arrays.asList("Sean", "Julian"));
        assertEquals(expectedData, record.bins);


        PreparedStatement select = testConn.prepareStatement("select id, first_name, last_name, kids from people where PK=?");
        select.setInt(1, 1);
        ResultSet rs = select.executeQuery();
        assertTrue(rs.next());

        assertEquals(1, rs.getInt(1));
        assertEquals(1, rs.getInt("id"));
        assertEquals("John", rs.getString(2));
        assertEquals("John", rs.getString("first_name"));
        assertEquals("Lennon", rs.getString(3));
        assertEquals("Lennon", rs.getString("last_name"));

        List<String> expectedKids = Arrays.asList("Sean", "Julian");
        assertEquals(expectedKids, rs.getObject(4));
        assertEquals(expectedKids, rs.getObject("kids"));

        assertEquals(expectedKids, Arrays.asList((Object[])rs.getArray(4).getArray()));
        assertEquals(expectedKids, Arrays.asList((Object[])rs.getArray("kids").getArray()));


        ResultSet arrayRs = rs.getArray(4).getResultSet();
        assertTrue(arrayRs.next());
        assertEquals(1, arrayRs.getInt(1));
        assertEquals("Sean", arrayRs.getString(2));
        assertTrue(arrayRs.next());
        assertEquals(2, arrayRs.getInt(1));
        assertEquals("Julian", arrayRs.getString(2));
        assertFalse(arrayRs.next());


        assertFalse(rs.next());


    }

}
