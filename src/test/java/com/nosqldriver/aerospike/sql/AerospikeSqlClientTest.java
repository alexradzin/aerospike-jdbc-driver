package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AerospikeSqlClientTest {
    @Test
    void successfulInit() {
        new AerospikeSqlClient(() -> TestDataUtils.client);
    }

    @Test
    void unsuccessfulInitWrongHost() {
        Assertions.assertThrows(SQLException.class, () -> new AerospikeSqlClient(() -> new AerospikeClient("wronghost", 3000)));
    }

    @Test
    void unsuccessfulInitWrongPort() {
        Assertions.assertThrows(SQLException.class, () -> new AerospikeSqlClient(() -> new AerospikeClient("localhost", 3456)));
    }

    @Test
    void simpleGetters() {
        IAerospikeClient realClient = TestDataUtils.client;
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> realClient);
        assertEquals(realClient.getReadPolicyDefault(), wrapperClient.getReadPolicyDefault());
        assertEquals(realClient.getWritePolicyDefault(), wrapperClient.getWritePolicyDefault());
        assertEquals(realClient.getScanPolicyDefault(), wrapperClient.getScanPolicyDefault());
        assertEquals(realClient.getQueryPolicyDefault(), wrapperClient.getQueryPolicyDefault());
        assertEquals(realClient.getBatchPolicyDefault(), wrapperClient.getBatchPolicyDefault());
        assertEquals(realClient.getInfoPolicyDefault(), wrapperClient.getInfoPolicyDefault());

        assertEquals(realClient.isConnected(), wrapperClient.isConnected());
        assertArrayEquals(realClient.getNodes(), wrapperClient.getNodes());
        assertEquals(realClient.getNodeNames(), wrapperClient.getNodeNames());

        for(String nodeName : realClient.getNodeNames()) {
            assertEquals(realClient.getNode(nodeName), wrapperClient.getNode(nodeName));
        }

        // ClusterStats does not implement equals but implements consistent toString that can be used for comparison
        assertEquals(realClient.getClusterStats().toString(), wrapperClient.getClusterStats().toString());

        Key notExistingKey = new Key("test", "people", "does not exist");
        assertEquals(realClient.exists(null, notExistingKey), wrapperClient.exists(null, notExistingKey));
        assertArrayEquals(realClient.exists(null, new Key[] {notExistingKey}), wrapperClient.exists(null, new Key[] {notExistingKey}));
    }


    @Test
    void callOnClosedClient() {
        IAerospikeClient realClient = new AerospikeClient("localhost", 3000); // This test closes client, so it needs its own instance to avoid failures of other tests
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> realClient);
        callOnClosedClient(realClient, AerospikeException.class);
        callOnClosedClient(wrapperClient, SQLException.class);
    }


    private <T extends Throwable> void callOnClosedClient(IAerospikeClient client, Class<T> exceptionType) {
        client.close();
        client.getNodeNames(); // No validation here because this method returns empty list on build machine and non empty list on my computer. I do not know why.
        Key notExistingKey = new Key("test", "people", "does not exist");
        assertThrows(exceptionType, () -> client.exists(null, notExistingKey));
        assertThrows(exceptionType, () -> client.exists(null, new Key[] {notExistingKey}));
    }
}