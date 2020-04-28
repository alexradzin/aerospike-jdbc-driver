package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexType;
import com.nosqldriver.Person;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.util.ThrowingFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.aerospike.client.Log.setCallback;
import static com.aerospike.client.Log.setLevel;
import static com.nosqldriver.util.SneakyThrower.sneakyThrow;
import static java.lang.String.format;
import static org.junit.Assert.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * This class contains various utilities needed for easier manipulations with data in DB.
 */
public class TestDataUtils {
    @VisibleForPackage static final String NAMESPACE = "test";
    @VisibleForPackage static final String PEOPLE = "people";
    @VisibleForPackage static final String INSTRUMENTS = "instruments";
    @VisibleForPackage static final String GUITARS = "guitars";
    @VisibleForPackage static final String KEYBOARDS = "keyboards";
    @VisibleForPackage static final String SUBJECT_SELECTION = "subject_selection";
    @VisibleForPackage static final String DATA = "data";
    @VisibleForPackage static final String SELECT_ALL = "select * from people";

    static {
        setCallback((level, message) -> System.out.println(message));
        setLevel(Log.Level.DEBUG);
    }
    static final String aerospikeHost = System.getProperty("aerospike.host", "localhost");
    static final int aerospikePort = Integer.parseInt(System.getProperty("aerospike.port", "3000"));
    static final String aerospikeRootUrl = format("jdbc:aerospike:%s:%d", aerospikeHost, aerospikePort);
    static final String aerospikeTestUrl = format("jdbc:aerospike:%s:%d/test", aerospikeHost, aerospikePort);

    private static AerospikeClient client;
    private static Connection testConn = null;


    @VisibleForPackage
    static IAerospikeClient getClient() {
        if (client == null) {
            synchronized (TestDataUtils.class) {
                if (client == null) {
                    client = new AerospikeClient(aerospikeHost, aerospikePort);
                    closeOnShutdown(client);
                }
            }
        }
        return client;
    }

    @VisibleForPackage
    static Connection getTestConnection() {
        if (testConn == null) {
            synchronized (TestDataUtils.class) {
                if (testConn == null) {
                    try {
                        testConn = DriverManager.getConnection(aerospikeTestUrl);
                    } catch (SQLException e) {
                        throw new IllegalStateException("Cannot create connection to DB", e);
                    }
                }
            }
        }
        return testConn;
    }

    private static void closeOnShutdown(AutoCloseable c) {
        // Just to be polite. Open connection should be closed. The connection and client however are static and shared among all tests,
        // so we cannot close it in "@After" or "@AfterAll" of any test case. So, we do our best and close it at least here.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    c.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public static final Person[] beatles = new Person[] {
            new Person(1, "John", "Lennon", 1940, 2),
            new Person(2, "Paul", "McCartney", 1942, 5),
            new Person(3, "George", "Harrison", 1943, 1),
            new Person(4, "Ringo", "Starr", 1940, 3),
    };


    @VisibleForPackage static Function<String, Integer> executeUpdate = new Function<String, Integer>() {
        @Override
        public Integer apply(String sql) {
            try {
                return getTestConnection().createStatement().executeUpdate(sql);
            } catch (SQLException e) {
                return sneakyThrow(e);
            }
        }
    };

    @VisibleForPackage static Function<String, Boolean> execute = new Function<String, Boolean>() {
        @Override
        public Boolean apply(String sql) {
            try {
                return getTestConnection().createStatement().execute(sql);
            } catch (SQLException e) {
                return sneakyThrow(e);
            }
        }
    };

    @VisibleForPackage static Function<String, ResultSet> executeQuery = new Function<String, ResultSet>() {
        @Override
        public ResultSet apply(String sql) {
            try {
                return getTestConnection().createStatement().executeQuery(sql);
            } catch (SQLException e) {
                return sneakyThrow(e);
            }
        }
    };

    @VisibleForPackage static Function<String, ResultSet> executeQueryPreparedStatement = new Function<String, ResultSet>() {
        @Override
        public ResultSet apply(String sql) {
            try {
                return getTestConnection().prepareStatement(sql).executeQuery();
            } catch (SQLException e) {
                return sneakyThrow(e);
            }
        }
    };

    @VisibleForPackage static BiFunction<String, Object[], ResultSet> executeQueryPreparedStatementWithParameters = new BiFunction<String, Object[], ResultSet>() {
        @Override
        public ResultSet apply(String sql, Object[] params) {
            try {
                PreparedStatement ps = getTestConnection().prepareStatement(sql);
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
                return ps.executeQuery();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    };


    @VisibleForPackage static boolean resultSetNext(ResultSet rs) {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @VisibleForPackage static Collection<String> retrieveColumn(String sql, String column) throws SQLException {
        ResultSet rs = getTestConnection().createStatement().executeQuery(sql);
        Collection<String> data = new HashSet<>();
        while (rs.next()) {
            data.add(rs.getString(column));
        }
        return data;
    }

    @VisibleForPackage static void writeBeatles() {
        writeBeatles(new WritePolicy());
    }

    @VisibleForPackage static void writeBeatles(WritePolicy writePolicy) {
        write(PEOPLE, writePolicy, 1, person(1, "John", "Lennon", 1940, 2));
        write(PEOPLE, writePolicy, 2, person(2, "Paul", "McCartney", 1942, 5));
        write(PEOPLE, writePolicy, 3, person(3, "George", "Harrison", 1943, 1));
        write(PEOPLE, writePolicy, 4, person(4, "Ringo", "Starr", 1940, 3));
    }


    @VisibleForPackage static void writeData() {
        WritePolicy writePolicy = new WritePolicy();
        write(writePolicy, new Key(NAMESPACE, DATA, "one"), new Bin("string", "hello"));
    }



    //Juliawrin Lennon 1963
    //Sean Lennon 1975
    // Heather McCartney, 1962
    // Mary McCartney, 1969
    // Stella McCartney, 1971
    // James McCartney, 1977
    // Dhani Harrison, 1978
    // Zak Starkey, 1965

    @VisibleForPackage static void writeSubjectSelection() {
        //reference: https://stackoverflow.com/questions/2421388/using-group-by-on-multiple-columns
        WritePolicy writePolicy = new WritePolicy();
        int id = 1;
        write(SUBJECT_SELECTION, writePolicy, id++, subjectSelection("ITB001", 1, "John"));
        write(SUBJECT_SELECTION, writePolicy, id++, subjectSelection("ITB001", 1, "Bob"));
        write(SUBJECT_SELECTION, writePolicy, id++, subjectSelection("ITB001", 1, "Mickey"));
        write(SUBJECT_SELECTION, writePolicy, id++, subjectSelection("ITB001", 2, "Jenny"));
        write(SUBJECT_SELECTION, writePolicy, id++, subjectSelection("ITB001", 2, "James"));
        write(SUBJECT_SELECTION, writePolicy, id++, subjectSelection("MKB114", 1, "John"));
        write(SUBJECT_SELECTION, writePolicy, id, subjectSelection("MKB114", 1, "Erica"));
    }

    @VisibleForPackage static void writeMainPersonalInstruments() {
        writeMainPersonalInstruments(new WritePolicy());
    }

    @VisibleForPackage static void writeMainPersonalInstruments(WritePolicy writePolicy) {
        write(INSTRUMENTS, writePolicy, 1, personalInstrument(1, 1, "guitar"));
        write(INSTRUMENTS, writePolicy, 2, personalInstrument(2, 2, "bass guitar"));
        write(INSTRUMENTS, writePolicy, 3, personalInstrument(3, 3, "guitar"));
        write(INSTRUMENTS, writePolicy, 4, personalInstrument(4, 4, "drums"));
    }

    @VisibleForPackage static void writeAllPersonalInstruments() {
        WritePolicy writePolicy = new WritePolicy();
        // John Lennon
        write(INSTRUMENTS, writePolicy, 1, personalInstrument(1, 1, "vocals"));
        write(INSTRUMENTS, writePolicy, 2, personalInstrument(2, 1, "guitar"));
        write(INSTRUMENTS, writePolicy, 3, personalInstrument(3, 1, "keyboards"));
        write(INSTRUMENTS, writePolicy, 4, personalInstrument(4, 1, "harmonica"));

        // Paul McCartney
        write(INSTRUMENTS, writePolicy, 5, personalInstrument(5, 2, "vocals"));
        write(INSTRUMENTS, writePolicy, 6, personalInstrument(6, 2, "bass guitar"));
        write(INSTRUMENTS, writePolicy, 7, personalInstrument(7, 2, "guitar"));
        write(INSTRUMENTS, writePolicy, 8, personalInstrument(8, 2, "keyboards"));

        // George Harrison
        write(INSTRUMENTS, writePolicy, 9, personalInstrument(9, 3, "vocals"));
        write(INSTRUMENTS, writePolicy, 10, personalInstrument(10, 3, "guitar"));
        write(INSTRUMENTS, writePolicy, 11, personalInstrument(11, 3, "sitar"));

        // Ringo Starr
        write(INSTRUMENTS, writePolicy, 12, personalInstrument(12, 4, "drums"));
        write(INSTRUMENTS, writePolicy, 13, personalInstrument(13, 4, "vocals"));
    }

    @VisibleForPackage static void writeGuitars() {
        WritePolicy writePolicy = new WritePolicy();
        write(GUITARS, writePolicy, 2, personalInstrument(2, 1, "guitar")); // John Lennon
        write(GUITARS, writePolicy, 7, personalInstrument(7, 2, "guitar")); // Paul McCartney
        write(GUITARS, writePolicy, 10, personalInstrument(10, 3, "guitar")); // George Harrison
    }


    @VisibleForPackage static void writeKeyboards() {
        WritePolicy writePolicy = new WritePolicy();
        write(KEYBOARDS, writePolicy, 3, personalInstrument(3, 1, "keyboards")); // John Lennon
        write(KEYBOARDS, writePolicy, 8, personalInstrument(8, 2, "keyboards")); // Paul McCartney
    }

    @VisibleForPackage static void write(WritePolicy writePolicy, Key key, Bin... bins) {
        getClient().put(writePolicy, key, bins);
    }

    private static void write(String table, WritePolicy writePolicy, int id, Bin ... bins) {
        write(writePolicy, new Key(NAMESPACE, table, id), bins);
    }

    @VisibleForPackage static Bin[] person(int id, String firstName, String lastName, int yearOfBirth, int kidsCount) {
        return new Bin[] {new Bin("id", id), new Bin("first_name", firstName), new Bin("last_name", lastName), new Bin("year_of_birth", yearOfBirth), new Bin("kids_count", kidsCount)};
    }

    private static Bin[] subjectSelection(String subject, int semester, String attendee) {
        return new Bin[] {new Bin("subject", subject), new Bin("semester", semester), new Bin("attendee", attendee)};
    }

    private static Bin[] personalInstrument(int id, int personId, String name) {
        return new Bin[] {new Bin("id", id), new Bin("person_id", personId), new Bin("name", name)};
    }

    @VisibleForPackage static void deleteAllRecords(String namespace, String table) {
        getClient().scanAll(new ScanPolicy(), namespace, table, (key, record) -> getClient().delete(new WritePolicy(), key));
    }

    @VisibleForPackage static void dropIndexSafely(String fieldName) {
        try {
            dropIndex(fieldName);
        } catch (AerospikeException e) {
            if (e.getResultCode() != 201) {
                throw e;
            }
        }
    }

    @VisibleForPackage static void createIndex(String fieldName, IndexType indexType) {
        getClient().createIndex(null, NAMESPACE, PEOPLE, getIndexName(fieldName), fieldName, indexType).waitTillComplete();
    }

    private static void dropIndex(String fieldName) {
        getClient().dropIndex(null, NAMESPACE, PEOPLE, getIndexName(fieldName)).waitTillComplete();
    }

    @VisibleForPackage static Collection<String> getIndexes() {
        return new ConnectionParametersParser().indexesParser(Info.request(getClient().getNodes()[0], "sindex"));
    }

    private static String getIndexName(String fieldName) {
        return format("%s_%s_INDEX", PEOPLE, fieldName.toUpperCase());
    }


    @VisibleForPackage
    static ResultSet executeQuery(String sql, String expectedSchema, boolean orderedValidation, Object ... expectedMetadataFields) throws SQLException {
        Statement statement = getTestConnection().createStatement();
        ResultSet rs = statement.executeQuery(sql);
        assertSame(statement, rs.getStatement());

        if (expectedMetadataFields.length > 0) {
            validate(rs.getMetaData(), expectedSchema, orderedValidation, expectedMetadataFields);
        }

        return rs;
    }


    @VisibleForPackage
    static ResultSet executeQuery(String sql, DataColumn... expectedColumns) throws SQLException {
        return executeQuery(getTestConnection(), sql, expectedColumns);
    }

    @VisibleForPackage
    static ResultSet executeQuery(Connection connection, String sql, DataColumn... expectedColumns) throws SQLException {
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        assertSame(statement, rs.getStatement());

        if (expectedColumns.length > 0) {
            validate(rs.getMetaData(), expectedColumns);
        }

        return rs;
    }


    /**
     * Validates metadata against expected values
     * @param md
     * @param expectedMetadataFields expected fields: name,label,type
     * @return the metadata sent as an argument
     * @throws SQLException
     */
    @VisibleForPackage
    static ResultSetMetaData validate(ResultSetMetaData md, String expectedSchema, boolean orderedValidation, Object ... expectedMetadataFields) throws SQLException {
        assertNotNull(md);

        Map<String, String> expectedName2label = new HashMap<>();
        Map<String, Integer> expectedName2type = new HashMap<>();
        Map<String, String> name2label = new HashMap<>();
        Map<String, Integer> name2type = new HashMap<>();
        int n = expectedMetadataFields.length / 3;
        assertEquals(n, md.getColumnCount());
        for (int i = 1, j = 0; i <= n; i++, j+=3) {
            assertEquals(expectedSchema, md.getCatalogName(i));
            assertEquals("", md.getSchemaName(i));
            String name = (String)expectedMetadataFields[j];
            String label = (String)expectedMetadataFields[j + 1];
            Integer type = (Integer)expectedMetadataFields[j + 2];
            if (orderedValidation) {
                if (name != null) {
                    assertEquals(name, md.getColumnName(i));
                }
                if (label != null) {
                    assertEquals(label, md.getColumnLabel(i));
                }
                if (type != null) {
                    assertEquals(type.intValue(), md.getColumnType(i));
                }
            } else if(name != null && md.getColumnName(i) != null) {
                if (label != null) {
                    expectedName2label.put(name, label);
                    name2label.put(md.getColumnName(i), md.getColumnLabel(i));
                }
                if (type != null) {
                    expectedName2type.put(name, type);
                    name2type.put(name, type);
                }
            }
        }

        if (!orderedValidation) {
            assertEquals(expectedName2label, name2label);
            assertEquals(expectedName2type, name2type);
        }
        return md;
    }

    public static ResultSetMetaData validate(ResultSetMetaData md, DataColumn ... expectedColumns) throws SQLException {
        assertNotNull(md);
        System.out.println("Column names: " + Arrays.stream(expectedColumns).map(DataColumn::getName).toString());
        assertEquals(expectedColumns.length, md.getColumnCount(), "Column names: " + Arrays.stream(expectedColumns).map(DataColumn::getName).toString());

        for (int i = 0; i < expectedColumns.length; i++) {
            int j = i + 1;
            assertEquals("", md.getSchemaName(j));
            assertEquals(expectedColumns[i].getCatalog(), md.getCatalogName(j));
            assertEquals(expectedColumns[i].getTable(), md.getTableName(j));
            assertEquals(expectedColumns[i].getName(), md.getColumnName(j));
            assertEquals(expectedColumns[i].getLabel(), md.getColumnLabel(j));
            assertEquals(expectedColumns[i].getType(), md.getColumnType(j), format("Wrong type of %s.%s.%s", md.getSchemaName(j), md.getTableName(j), md.getColumnLabel(j)));
        }

        return md;
    }

    public static Collection<Map<String, Object>> toListOfMaps(ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();

        Collection<Map<String, Object>> result = new ArrayList<>();
        while (rs.next()) {
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 1; i <= n; i++) {
                map.put(md.getColumnLabel(i), rs.getObject(i));
            }
            result.add(map);
        }
        return result;
    }

    public static void assertFindColumn(ResultSet rs, String ... columns) throws SQLException {
        for (int i = 0; i < columns.length; i++) {
            if (columns[i] != null) {
                assertEquals(i + 1, rs.findColumn(columns[i]));
            }
        }
        assertEquals(rs.getMetaData().getColumnCount(), columns.length);
    }

    public static <T> Collection<T> getColumnValues(ResultSet rs, ThrowingFunction<ResultSet, T, SQLException> getter) throws SQLException {
        Collection<T> values = new ArrayList<>();
        while (rs.next()) {
            values.add(getter.apply(rs));
        }
        return values;
    }
}
