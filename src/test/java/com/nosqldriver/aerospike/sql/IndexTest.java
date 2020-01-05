package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.nosqldriver.util.ThrowingBiFunction;
import com.nosqldriver.util.ThrowingConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.testConn;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class IndexTest {
    private static final String STRING_INDEX_NAME = "PEOPLE_FIRST_NAME_INDEX";
    private static final String NUMERIC_INDEX_NAME = "PEOPLE_YOB_INDEX";
    private static final String LIST_INDEX_NAME = "DATA_LIST_INDEX1";
    private static final String MAPKEYS_INDEX_NAME = "MAPKEYS_INDEX";
    private static final String MAPVALUES_INDEX_NAME = "MAPVALUES_INDEX";
    private static final String GEO_INDEX_NAME = "GEO_INDEX1";

    private static final String[] INDEX_NAMES = new String[] {STRING_INDEX_NAME, NUMERIC_INDEX_NAME, LIST_INDEX_NAME, MAPKEYS_INDEX_NAME, MAPVALUES_INDEX_NAME, GEO_INDEX_NAME};


    @BeforeAll
    @AfterAll
    static void dropAll() {
        TestDataUtils.deleteAllRecords(NAMESPACE, PEOPLE);
        Arrays.stream(INDEX_NAMES).forEach(TestDataUtils::dropIndexSafely);
    }


    @BeforeEach
    @AfterEach
    void clean() {
        try {
            TestDataUtils.client.dropIndex(null, NAMESPACE, PEOPLE, STRING_INDEX_NAME).waitTillComplete();
        } catch (AerospikeException e) {
            if (e.getResultCode() != 201) {
                throw e;
            }
        }
        try {
            TestDataUtils.client.dropIndex(null, NAMESPACE, PEOPLE, NUMERIC_INDEX_NAME).waitTillComplete();
        } catch (AerospikeException e) {
            if (e.getResultCode() != 201) {
                throw e;
            }
        }
    }



    @Test
    void createAndDropStringIndexUsingExecute() throws SQLException, IOException {
        assertCreateAndDropIndex("people", "first_name", STRING_INDEX_NAME, "STRING", Statement::execute, Assertions::assertTrue);
    }

    @Test
    void createAndDropNumericIndexUsingExecute() throws SQLException, IOException {
        assertCreateAndDropIndex("people", "year_of_birth", NUMERIC_INDEX_NAME, "NUMERIC", Statement::execute, Assertions::assertTrue);
    }

    @Test
    void createAndDropListIndexUsingExecute() throws SQLException, IOException {
        assertCreateAndDropIndex("data", "list", LIST_INDEX_NAME, "NUMERIC LIST", Statement::execute, Assertions::assertTrue);
    }

    @Test
    void createAndDropGeoIndexUsingExecute() throws SQLException, IOException {
        assertCreateAndDropIndex("data", "location1", GEO_INDEX_NAME, "GEO2DSPHERE", Statement::execute, Assertions::assertTrue);
    }

    @Test
    void createAndDropStringMapkeysIndexUsingExecute() throws SQLException, IOException {
        assertCreateAndDropIndex("data", "mapkeys", MAPKEYS_INDEX_NAME, "STRING MAPKEYS", Statement::execute, Assertions::assertTrue);
    }

    @Test
    void createAndDropStringMapvalesIndexUsingExecute() throws SQLException, IOException {
        assertCreateAndDropIndex("data", "mapkeys", MAPVALUES_INDEX_NAME, "STRING MAPVALUES", Statement::execute, Assertions::assertTrue);
    }

    @Test
    void createAndDropStringIndexUsingExecuteUpdate() throws SQLException, IOException {
        assertCreateAndDropIndex("people", "first_name", STRING_INDEX_NAME, "STRING", Statement::executeUpdate, r -> assertEquals(1, r.intValue()));
    }

    @Test
    void createAndDropNumericIndexUsingExecuteUpdate() throws SQLException, IOException {
        assertCreateAndDropIndex("people", "year_of_birth", NUMERIC_INDEX_NAME, "NUMERIC", Statement::executeUpdate, r -> assertEquals(1, r.intValue()));
    }

    @Test
    void createAndDropStringIndexUsingExecuteQuery() throws SQLException, IOException {
        assertCreateAndDropIndex("people", "first_name", STRING_INDEX_NAME, "STRING", Statement::executeQuery, rs -> assertFalse(rs.next()));
    }

    @Test
    void createAndDropNumericIndexUsingExecuteQuery() throws SQLException, IOException {
        assertCreateAndDropIndex("people", "year_of_birth", NUMERIC_INDEX_NAME, "NUMERIC", Statement::executeQuery, rs -> assertFalse(rs.next()));
    }


    private <R> void assertCreateAndDropIndex(String table, String column, String indexName, String indexType, ThrowingBiFunction<Statement, String, R, SQLException> executor, ThrowingConsumer<R, SQLException> validator) throws SQLException, IOException {
        TestDataUtils.writeBeatles();
        await().atMost(5, SECONDS).until(() -> !Info.request(TestDataUtils.client.getNodes()[0], "sindex").contains(indexName));
        validator.accept(executor.apply(testConn.createStatement(), format("CREATE %s INDEX %s ON %s (%s)", indexType, indexName, table, column)));

        Properties indexProps = new Properties();
        indexProps.load(new StringReader(Info.request(TestDataUtils.client.getNodes()[0], "sindex").replace(':', '\n')));
        assertEquals(indexName, indexProps.getProperty("indexname"));

        validator.accept(executor.apply(testConn.createStatement(), format("DROP INDEX %s.%s", table, indexName)));
        assertFalse(TestDataUtils.getIndexes().contains(indexName));

    }

}
