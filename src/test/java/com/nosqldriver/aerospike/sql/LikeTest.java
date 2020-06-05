package com.nosqldriver.aerospike.sql;

import com.nosqldriver.sql.ScriptEngineFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

import static com.nosqldriver.aerospike.sql.TestDataUtils.DATA;
import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.deleteAllRecords;
import static com.nosqldriver.aerospike.sql.TestDataUtils.getTestConnection;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;


class DefaultLikeTest extends LikeTest {
    DefaultLikeTest() {
        super(getTestConnection());
    }

    static Connection createConnection(String url) {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}

class JsLikeTest extends LikeTest {
    private final static Connection jsConnection = DefaultLikeTest.createConnection(TestDataUtils.aerospikeTestUrlJs);

    JsLikeTest() {
        super(jsConnection);
    }
}

class LuaLikeTest extends LikeTest {
    private final static Connection luaConnection = DefaultLikeTest.createConnection(TestDataUtils.aerospikeTestUrlLua);

    LuaLikeTest() {
        super(luaConnection);
    }
}


abstract class LikeTest {
    private Connection conn;

    LikeTest(Connection conn) {
        this.conn = conn;
    }

    @BeforeAll
    static void setUp() throws SQLException {
        dropAll();
        getTestConnection().createStatement().executeUpdate("insert into data (PK, text) values (1, 'abc'), (2, 'abcd'), (3, 'bcd')");
        ScriptEngineFactory.cleanup();
    }



    static void dropAll() throws SQLException {
        deleteAllRecords(NAMESPACE, DATA);
    }

    @AfterAll
    static void dropAllRecords() throws SQLException {
        dropAll();
        ScriptEngineFactory.cleanup();
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {
            "select text from data;abc,abcd,bcd",
            "select text from data where text like '%c%';abc,abcd,bcd",
            "select text from data where text like 'a%';abc,abcd",
            "select text from data where text like '%a%';abc,abcd",
            "select text from data where text like '%c';abc",
            "select text from data where text like '%d';abcd,bcd",
            "select text from data where text like '%bcd';abcd,bcd",
            "select text from data where text like 'a';",
            "select text from data where text like '%a';",
    })
    void selectLike(String queryAndResult) throws SQLException {
        String[] parts = queryAndResult.split(";");
        String sql = parts[0];
        Collection<String> expected = parts.length == 1 ? Collections.emptySet() : new HashSet<>(asList(parts[1].split(",")));

        ResultSet rs = conn.createStatement().executeQuery(sql);
        Collection<String> actual  = new HashSet<>();
        while(rs.next()) {
            actual.add(rs.getString(1));
        }
        assertEquals(expected, actual);

    }
}
