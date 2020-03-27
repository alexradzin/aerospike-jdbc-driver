package com.nosqldriver.aerospike.sql;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.DataColumn.DataColumnRole;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.sql.TypeDiscoverer;
import com.nosqldriver.util.FunctionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.nosqldriver.aerospike.sql.TestDataUtils.DATA;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getClient;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Although this is a test of GenericTypeDiscoverer that belongs to package {@code com.nosqldriver.sql}
 * the test itself uses Aerospike and therefore is located here, in {@code com.nosqldriver.aerospike.sql}.
 */
class GenericTypeDiscovererTest {
    private static final BiFunction<String, String, Iterable<KeyRecord>> recordsFetcher = (catalog, table) -> {
        Statement statement = new Statement();
        statement.setNamespace(catalog);
        statement.setSetName(table);
        return getClient().query(new QueryPolicy(), statement);
    };
    private static final Function<KeyRecord, Map<String, Object>> recordDataExtractor = keyRecord -> keyRecord.record.bins;
    private FunctionManager functionManager = new FunctionManager();
    private Connection testConn = getTestConnection();

    @AfterEach
    void dropAll() {
        deleteAllRecords(NAMESPACE, DATA);
    }

    @Test
    void discoverNoColumnsEmptyTable() {
        assertDiscoveredColumns(createTypeDiscoverer(), emptyList(), emptySet());
    }

    @Test
    void discoverAllEmptyTable() {
        assertDiscoveredColumns(createTypeDiscoverer(), singletonList(DataColumnRole.DATA.create(NAMESPACE, DATA, "*", "*")), emptySet());
    }

    @Test
    void discoverAllOneColumn() throws SQLException {
        assertEquals(1, testConn.createStatement().executeUpdate("insert into data (PK, name) values (1, 'Adam')"));
        assertEquals(1, testConn.createStatement().executeUpdate("insert into data (PK, name) values (2, 'Eve')"));
        assertDiscoveredColumns(createTypeDiscoverer(), singletonList(DataColumnRole.DATA.create(NAMESPACE, DATA, "*", "*")), singleton(DataColumnRole.DATA.create(NAMESPACE, DATA, "name", "name").withType(Types.VARCHAR)));
    }

    @Test
    void discoverAll() throws SQLException {
        writeData();
        List<DataColumn> all = singletonList(DataColumnRole.DATA.create(NAMESPACE, DATA, "*", "*"));
        assertDiscoveredColumns(createTypeDiscoverer(), all);
        assertDiscoveredColumns(createTypeDiscoverer(4), all);
    }

    @Test
    void assertDiscoveredColumns() throws SQLException {
        writeData();
        List<DataColumn> all = asList(
                DataColumnRole.DATA.create(NAMESPACE, DATA, "name", "name"),
                DataColumnRole.DATA.create(NAMESPACE, DATA, "mother", "mother"),
                DataColumnRole.DATA.create(NAMESPACE, DATA, "father", "father"));
        assertDiscoveredColumns(createTypeDiscoverer(), all);
        assertDiscoveredColumns(createTypeDiscoverer(10), all);
    }


    void assertDiscoveredColumns(TypeDiscoverer discoverer, List<DataColumn> columns) {
        assertDiscoveredColumns(
                discoverer,
                columns,
                new HashSet<>(asList(
                        DataColumnRole.DATA.create(NAMESPACE, DATA, "name", "name").withType(Types.VARCHAR),
                        DataColumnRole.DATA.create(NAMESPACE, DATA, "mother", "mother").withType(Types.VARCHAR),
                        DataColumnRole.DATA.create(NAMESPACE, DATA, "father", "father").withType(Types.VARCHAR)
                )));
    }

    private void assertDiscoveredColumns(TypeDiscoverer discoverer, List<DataColumn> columns, Set<DataColumn> expected) {
        assertEquals(expected, new HashSet<>(discoverer.discoverType(columns)));
    }

    private void writeData() throws SQLException {
        assertEquals(1, testConn.createStatement().executeUpdate("insert into data (PK, name) values (1, 'Adam')"));
        assertEquals(1, testConn.createStatement().executeUpdate("insert into data (PK, name) values (2, 'Eve')"));
        assertEquals(1, testConn.createStatement().executeUpdate("insert into data (PK, name, mother, father) values (3, 'Cain', 'Eve', 'Adam')"));
        assertEquals(1, testConn.createStatement().executeUpdate("insert into data (PK, name, mother, father) values (4, 'Abel', 'Eve', 'Adam')"));
    }


    private TypeDiscoverer createTypeDiscoverer(int limit) {
        return new GenericTypeDiscoverer<>(recordsFetcher, recordDataExtractor, functionManager, limit, false);
    }

    private TypeDiscoverer createTypeDiscoverer() {
        return new GenericTypeDiscoverer<>(recordsFetcher, keyRecord -> keyRecord.record.bins, functionManager, false);
    }
}