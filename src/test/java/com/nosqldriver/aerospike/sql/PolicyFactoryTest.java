package com.nosqldriver.aerospike.sql;

import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Priority;
import com.aerospike.client.policy.QueryPolicy;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PolicyFactoryTest {
    @Test
    void copyPolicy() {
        Policy policy = new Policy();
        policy.totalTimeout = 1024;
        policy.maxRetries = 123;
        QueryPolicy queryPolicy = new QueryPolicy();
        PolicyFactory.copy(policy, queryPolicy);
        assertEquals(policy.totalTimeout, queryPolicy.totalTimeout);
        assertEquals(policy.maxRetries, queryPolicy.maxRetries);
    }

    @Test
    void copyProperties() {
        Properties props = new Properties();
        props.setProperty("totalTimeout", "2048");
        props.setProperty("linearizeRead", "true");
        props.setProperty("priority", "HIGH");

        QueryPolicy queryPolicy = new QueryPolicy();
        PolicyFactory.copy(props, queryPolicy);
        assertEquals(queryPolicy.totalTimeout, queryPolicy.totalTimeout);
        assertTrue(queryPolicy.linearizeRead);
        assertEquals(Priority.HIGH, queryPolicy.priority);
    }

}