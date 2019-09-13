package com.nosqldriver.sql;

import com.nosqldriver.util.ByteArrayComparator;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.lang.System.currentTimeMillis;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResultSetHashExtractorTest {
    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";

    private final Function<ResultSet, byte[]> simpleExtractor = new ResultSetHashExtractor();

    @Test
    void hashAllTypes() throws SQLException {
        ResultSet rs = resultSet(currentTimeMillis());
        assertTrue(rs.next());
        byte[] hash = simpleExtractor.apply(rs);
        assertNotNull(hash);
    }


    @Test
    void filteredNotEqualToFull() throws SQLException {
        ResultSet rs = resultSet(currentTimeMillis());
        assertTrue(rs.next());
        byte[] all = simpleExtractor.apply(rs);
        byte[] text = new ResultSetHashExtractor("text"::equals).apply(rs);

        assertNotNull(all);
        assertNotNull(text);
        assertFalse(new ByteArrayComparator().compare(all, text) == 0);
    }

    @Test
    void filteredDoesNotDependOnOhterFields() throws SQLException {
        long now = currentTimeMillis();
        ResultSet rs1 = resultSet(now);

        ResultSet rs2 = resultSet(now - 3600_000);
        assertTrue(rs1.next());
        assertTrue(rs2.next());
        byte[] text1 = new ResultSetHashExtractor("text"::equals).apply(rs1);
        byte[] text2 = new ResultSetHashExtractor("text"::equals).apply(rs2);

        assertArrayEquals(text1, text2);
    }



    private DataColumn column(String name, int type) {
        return DATA.create(CATALOG, TABLE, name, name).withType(type);
    }


    private ResultSet resultSet(long now) {
        return new ListRecordSet("", "",
                asList(
                        column("flag", BOOLEAN),
                        column("num1", SMALLINT),
                        column("num2", INTEGER),
                        column("val1", FLOAT),
                        column("val2", DOUBLE),
                        column("text", VARCHAR),
                        column("d", DATE),
                        column("t", TIME),
                        column("ts", TIMESTAMP)
                ),
                singletonList(asList(true, 1, 1234356, 3.14, 2.71828, "hello", new java.sql.Date(now), new java.sql.Time(now), new java.sql.Timestamp(now)))
        );
    }
}