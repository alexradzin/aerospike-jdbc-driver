package com.nosqldriver.aerospike.sql;

import org.junit.jupiter.api.Test;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeHost;
import static com.nosqldriver.aerospike.sql.TestDataUtils.aerospikeRootUrl;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AerospikeDriverTest {
    @Test
    void init() {
        assertTrue(new AerospikeDriver().getMajorVersion() > -1); // enough that it does not fail
    }

    @Test
    void acceptsRightUrl() throws SQLException {
        assertTrue(new AerospikeDriver().acceptsURL(aerospikeRootUrl));
    }

    @Test
    void rejectsWrongUrl() throws SQLException {
        assertFalse(new AerospikeDriver().acceptsURL("jdbc:mysql:loclhost"));
    }

    @Test
    void successfulConnect() throws SQLException {
        assertNotNull(new AerospikeDriver().connect(aerospikeRootUrl, new Properties()));
    }

    @Test
    void unsuccessfulConnect() {
        assertThrows(SQLException.class, () -> new AerospikeDriver().connect("jdbc:aerospike:otherhost", new Properties()));
    }

    @Test
    void propertyInfoNoParameters() throws SQLException {
        assertEquals(0, new AerospikeDriver().getPropertyInfo("jdbc:aerospike:" + aerospikeHost, new Properties()).length);
    }

    @Test
    void propertyInfoUrlParameters() throws SQLException {
        DriverPropertyInfo[] info = new AerospikeDriver().getPropertyInfo(aerospikeRootUrl + "?timeout=12345", new Properties());
        assertEquals(1, info.length);
        assertEquals("timeout", info[0].name);
        assertEquals("12345", info[0].value);
    }

    @Test
    void propertyInfoProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("timeout", "54321");
        DriverPropertyInfo[] info = new AerospikeDriver().getPropertyInfo(aerospikeRootUrl, props);
        assertEquals(1, info.length);
        assertEquals("timeout", info[0].name);
        assertEquals("54321", info[0].value);
    }

    @Test
    void propertyInfoUrlAndProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("fetchSize", "1000");
        DriverPropertyInfo[] info = new AerospikeDriver().getPropertyInfo(aerospikeRootUrl + "?timeout=12345", props);
        assertEquals(2, info.length);
        assertEquals("timeout", info[0].name);
        assertEquals("12345", info[0].value);
        assertEquals("fetchSize", info[1].name);
        assertEquals("1000", info[1].value);
    }

    @Test
    void getMajorVersion() {
        assertEquals(1, new AerospikeDriver().getMajorVersion());
    }

    @Test
    void getMinorVersion() {
        assertEquals(0, new AerospikeDriver().getMinorVersion());
    }

    @Test
    void jdbcCompliant() {
        assertFalse(new AerospikeDriver().jdbcCompliant());
    }

    @Test
    void getParentLogger() {
        assertNotNull(new AerospikeDriver().getParentLogger());
    }
}