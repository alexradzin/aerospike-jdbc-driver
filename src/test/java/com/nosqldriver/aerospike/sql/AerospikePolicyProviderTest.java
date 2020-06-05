package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AerospikePolicyProviderTest {
    @Test
    void defaultPolicy() throws IOException {
        AerospikePolicyProvider provider = test("");
        assertNotNull(provider.getQueryPolicy());
        assertNotNull(provider.getScanPolicy());
        assertNotNull(provider.getBatchPolicy());
        assertNotNull(provider.getScanPolicy());
        assertNotNull(provider.getReadPolicy());
        assertNotNull(provider.getInfoPolicy());
    }

    @Test
    void queryPolicy() throws IOException {
        AerospikePolicyProvider provider = test(
                "policy.query.totalTimeout=12345\n" +
                "policy.query.maxConcurrentNodes=12\n" +
                "policy.query.includeBinData=false\n"
        );
        QueryPolicy p = provider.getQueryPolicy();
        assertEquals(12345, p.totalTimeout);
        assertEquals(12, p.maxConcurrentNodes);
        assertFalse(p.includeBinData);
    }

    @Test
    void generalPolicy() throws IOException {
        AerospikePolicyProvider p = test(
                "policy.*.totalTimeout=12345\n"
        );

        for (Policy policy : new Policy[] {p.getReadPolicy(), p.getScanPolicy(), p.getQueryPolicy(), p.getBatchPolicy(), p.getWritePolicy()}) {
            assertEquals(12345, policy.totalTimeout);
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "policy.*.totalTimeout=12345\npolicy.read.totalTimeout=54321",
            "policy.read.totalTimeout=54321\npolicy.*.totalTimeout=12345",
    })
    void generalAndSpecificConfiguration(String conf) throws IOException {
        AerospikePolicyProvider p = test(conf);

        for (Policy policy : new Policy[] {p.getScanPolicy(), p.getQueryPolicy(), p.getBatchPolicy(), p.getWritePolicy()}) {
            assertEquals(12345, policy.totalTimeout);
        }
        assertEquals(54321, p.getReadPolicy().totalTimeout);
    }

    private AerospikePolicyProvider test(String props) throws IOException {
        IAerospikeClient client = mock(IAerospikeClient.class);
        when(client.getReadPolicyDefault()).thenReturn(new Policy());
        when(client.getWritePolicyDefault()).thenReturn(new WritePolicy());
        when(client.getBatchPolicyDefault()).thenReturn(new BatchPolicy());
        when(client.getQueryPolicyDefault()).thenReturn(new QueryPolicy());
        when(client.getScanPolicyDefault()).thenReturn(new ScanPolicy());
        when(client.getInfoPolicyDefault()).thenReturn(new InfoPolicy());

        Properties properties = new Properties();
        properties.load(new StringReader(props));

        return new AerospikePolicyProvider(client, properties);
    }

}