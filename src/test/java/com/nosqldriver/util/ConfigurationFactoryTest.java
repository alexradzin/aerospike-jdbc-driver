package com.nosqldriver.util;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.QueryPolicy;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigurationFactoryTest {
    @Test
    void copyPolicy() {
        Policy policy = new Policy();
        policy.totalTimeout = 1024;
        policy.maxRetries = 123;
        QueryPolicy queryPolicy = new QueryPolicy();
        ConfigurationFactory.copy(policy, queryPolicy);
        assertEquals(policy.totalTimeout, queryPolicy.totalTimeout);
        assertEquals(policy.maxRetries, queryPolicy.maxRetries);
    }

    @Test
    void copySpecialToGeneralPolicy() {
        QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.totalTimeout = 1024;
        queryPolicy.maxRetries = 123;
        queryPolicy.maxConcurrentNodes = 345; // defined in QueryPolicy; absent in Policy
        Policy policy = new Policy();
        ConfigurationFactory.copy(queryPolicy, policy);
        assertEquals(policy.totalTimeout, queryPolicy.totalTimeout);
        assertEquals(policy.maxRetries, queryPolicy.maxRetries);
        assertEquals(345, queryPolicy.maxConcurrentNodes);
    }


    @Test
    void copyProperties() {
        Properties props = new Properties();
        props.setProperty("totalTimeout", "2048");
        props.setProperty("linearizeRead", "true");
        props.setProperty("priority", "HIGH");

        QueryPolicy queryPolicy = new QueryPolicy();
        ConfigurationFactory.copy(props, queryPolicy);
        assertEquals(queryPolicy.totalTimeout, queryPolicy.totalTimeout);
        assertTrue(queryPolicy.linearizeRead);
        assertEquals(Priority.HIGH, queryPolicy.priority);
    }

    @Test
    void copyNotExistingProperty() {
        Properties props = new Properties();
        props.setProperty("doesNotExist", "n/a"); // not existing property does not make the process to fail
        props.setProperty("totalTimeout", "2048");

        QueryPolicy queryPolicy = new QueryPolicy();
        ConfigurationFactory.copy(props, queryPolicy);
        assertEquals(queryPolicy.totalTimeout, queryPolicy.totalTimeout);
    }

    @Test
    void copyPropertyOfWrongType() {
        Properties props = new Properties();
        props.setProperty("list", "a, b, c");
        // Special case: WrongPolicy.list is of type List. Only primiteves and enums are supported; attempt to copy values to list should throw exception.
        WrongPolicy policy = new WrongPolicy();
        assertThrows(IllegalArgumentException.class, () -> ConfigurationFactory.copy(props, policy));
    }


    private static class WrongPolicy {
        public List<String> list;
    }
}
