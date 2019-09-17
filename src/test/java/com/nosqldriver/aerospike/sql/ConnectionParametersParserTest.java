package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Host;
import com.aerospike.client.policy.AuthMode;
import com.aerospike.client.policy.ClientPolicy;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionParametersParserTest {
    @Test
    void oneHostWithPort() {
        assertArrayEquals(
                new Host[] {new Host("myhost", 3210)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:myhost:3210"));
    }

    @Test
    void oneHostWithoutPort() {
        assertArrayEquals(
                new Host[] {new Host("somehost", 3000)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:somehost"));
    }

    @Test
    void severalHostWithoutPort() {
        assertArrayEquals(
                new Host[] {new Host("one", 3000), new Host("two", 3000), new Host("three", 3000)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:one,two,three"));
    }

    @Test
    void severalHostWithPort() {
        assertArrayEquals(
                new Host[] {new Host("first", 3100), new Host("second", 3200), new Host("third", 3300)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:first:3100,second:3200,third:3300"));
    }


    @Test
    void oneHostWithPortWithParameters() {
        assertArrayEquals(
                new Host[] {new Host("myhost", 3210)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:myhost:3210?timeout=1234"));
    }

    @Test
    void oneHostWithoutPortWithParameters() {
        assertArrayEquals(
                new Host[] {new Host("somehost", 3000)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:somehost?timeout=1234"));
    }

    @Test
    void severalHostWithoutPortWithParameters() {
        assertArrayEquals(
                new Host[] {new Host("one", 3000), new Host("two", 3000), new Host("three", 3000)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:one,two,three?timeout=1234"));
    }

    @Test
    void severalHostWithPortWithParameters() {
        assertArrayEquals(
                new Host[] {new Host("first", 3100), new Host("second", 3200), new Host("third", 3300)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:first:3100,second:3200,third:3300?timeout=1234"));
    }

    @Test
    void oneHostWithPortAndSchema() {
        assertArrayEquals(
                new Host[] {new Host("myhost", 3210)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:myhost:3210/test"));
    }

    @Test
    void oneHostWithPortSchemaAndParameter() {
        assertArrayEquals(
                new Host[] {new Host("myhost", 3210)},
                new ConnectionParametersParser().hosts("jdbc:aerospike:myhost:3210/test?timeout=4321"));
    }

    @Test
    void wrongUrl() {
        assertThrows(IllegalArgumentException.class, () -> new ConnectionParametersParser().hosts("jdbc:mysql:myhost:3210"));
    }

    @Test
    void parseNoIndexes() {
        Collection<String> indexes = new ConnectionParametersParser().indexesParser("");
        assertTrue(indexes.isEmpty());
    }

    @Test
    void parseOneIndex() {
        Collection<String> indexes = new ConnectionParametersParser().indexesParser("ns=test:set=people:indexname=PEOPLE_YOB_INDEX:bin=year_of_birth:type=NUMERIC:indextype=NONE:path=year_of_birth:state=RW;");
        assertEquals(1, indexes.size());
        assertEquals("NUMERIC.test.people.year_of_birth.PEOPLE_YOB_INDEX", indexes.iterator().next());
    }

    @Test
    void parseTwoIndex() {
        Collection<String> indexes = new HashSet<>(new ConnectionParametersParser().indexesParser(
                "ns=test:set=people:indexname=PEOPLE_YOB_INDEX:bin=year_of_birth:type=NUMERIC:indextype=NONE:path=year_of_birth:state=RW;ns=test:set=people:indexname=PEOPLE_FIRST_NAME_INDEX:bin=first_name:type=STRING:indextype=NONE:path=first_name:state=RW;"
        ));
        assertEquals(2, indexes.size());
        assertEquals(new HashSet<>(asList("NUMERIC.test.people.year_of_birth.PEOPLE_YOB_INDEX", "STRING.test.people.first_name.PEOPLE_FIRST_NAME_INDEX")), indexes);
    }

    @Test
    void clientPolicyNoParametersEmptyProperties() {
        ClientPolicy policy = clientPolicy("jdbc:aerospike:myhost:3210", new Properties());
        assertNull(policy.user);
        assertEquals(1000, policy.timeout);
        assertEquals(AuthMode.INTERNAL, policy.authMode);
        assertTrue(policy.failIfNotConnected);
    }

    @Test
    void clientPolicyWithParametersEmptyProperties() {
        ClientPolicy policy = clientPolicy("jdbc:aerospike:myhost:3210?user=admin&timeout=2000&authMode=EXTERNAL&failIfNotConnected=false", new Properties());
        assertEquals("admin", policy.user);
        assertEquals(2000, policy.timeout);
        assertEquals(AuthMode.EXTERNAL, policy.authMode);
        assertFalse(policy.failIfNotConnected);
    }

    @Test
    void clientPolicyWithoutParametersWithProperties() {
        Properties props = new Properties();
        props.setProperty("user", "admin");
        props.setProperty("timeout", "2000");
        props.setProperty("authMode", "EXTERNAL");
        props.setProperty("failIfNotConnected", "false");
        ClientPolicy policy = clientPolicy("jdbc:aerospike:myhost:3210", props);
        assertEquals("admin", policy.user);
        assertEquals(2000, policy.timeout);
        assertEquals(AuthMode.EXTERNAL, policy.authMode);
        assertFalse(policy.failIfNotConnected);
    }

    /**
     * Parameters given in URL override those given in properties.
     */
    @Test
    void clientPolicyWithDifferentParametersInUrlAndProperties() {
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("timeout", "2121");
        props.setProperty("authMode", "EXTERNAL");
        props.setProperty("failIfNotConnected", "false");
        ClientPolicy policy = clientPolicy("jdbc:aerospike:myhost:3210?user=admin&timeout=2345&authMode=EXTERNAL_INSECURE", props);
        assertEquals("admin", policy.user);
        assertEquals(2345, policy.timeout);
        assertEquals(AuthMode.EXTERNAL_INSECURE, policy.authMode);
        assertFalse(policy.failIfNotConnected);
    }

    private ClientPolicy clientPolicy(String url, Properties props) {
        return new ConnectionParametersParser().policy(url, props);
    }
}