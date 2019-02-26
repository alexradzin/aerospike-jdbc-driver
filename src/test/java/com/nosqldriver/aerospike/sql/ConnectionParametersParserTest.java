package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Host;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

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
}