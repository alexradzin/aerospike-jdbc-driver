package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.sql.Types.BIGINT;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DATE;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.FLOAT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.VARCHAR;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DelegatingResultSetTest {
    private static final String SCHEMA = "schema";
    private static final String TABLE = "table";

    private List<DataColumn> columns = Arrays.asList(
            column("short", SMALLINT), column("int", INTEGER), column("long", BIGINT),
            column("on", BOOLEAN), column("off", BOOLEAN),
            column("float", FLOAT), column("double", DOUBLE),
            column("text", VARCHAR),
            column("d", DATE), column("t", Types.TIME), column("ts", Types.TIMESTAMP)
    );

    private long now = currentTimeMillis();
    private Object[] row = new Object[] {(short)123, 123456, now, true, false, 3.14f, 3.1415926, "hello, world!", new java.sql.Date(now), new java.sql.Time(now), new java.sql.Timestamp(now)};
    private List<List<?>> data = singletonList(Arrays.asList(row));


    @Test
    void getAllTypesFilteredResultSetNoFilter() throws SQLException {
        getAllTypesNoFilter(new FilteredResultSet(new ListRecordSet("schema", "table", columns, data), columns, r -> true, true));
    }

    @Test
    void getAllTypesBufferedResultSetNoFilter() throws SQLException {
        getAllTypesNoFilter(new BufferedResultSet(new FilteredResultSet(new ListRecordSet("schema", "table", columns, data), columns, r -> true, true), new ArrayList<>()));
    }

    void getAllTypesNoFilter(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        assertEquals(row[0], rs.getShort(1));
        assertEquals(row[0], rs.getShort("short"));
        Short expShort = (short) row[0];
        assertEquals(expShort.intValue(), rs.getInt(1));
        assertEquals(expShort.longValue(), rs.getLong("short"));
        assertEquals(expShort.longValue(), rs.getLong(1));
        assertEquals(expShort.longValue(), rs.getLong("short"));

        assertEquals(row[1], rs.getInt(2));
        assertEquals(row[1], rs.getInt("int"));
        assertEquals(Integer.valueOf((int)row[1]).longValue(), rs.getLong(2));
        assertEquals(Integer.valueOf((int)row[1]).longValue(), rs.getLong("int"));

        assertEquals(row[2], rs.getLong(3));
        assertEquals(row[2], rs.getLong("long"));

        assertEquals(row[3], rs.getBoolean(4));
        assertEquals(row[3], rs.getBoolean("on"));
        assertEquals(row[4], rs.getBoolean(5));
        assertEquals(row[4], rs.getBoolean("off"));

        assertEquals((float)row[5], rs.getFloat(6));
        assertEquals((float)row[5], rs.getFloat("float"));
        assertEquals((float)row[5], rs.getDouble(6));
        assertEquals((float)row[5], rs.getDouble("float"));

        assertEquals((double)row[6], rs.getDouble(7));
        assertEquals((double)row[6], rs.getDouble("double"));

        assertEquals(row[7], rs.getString(8));
        assertEquals(row[7], rs.getString("text"));

        assertEquals(row[8], rs.getDate(9));
        assertEquals(row[8], rs.getDate("d"));

        assertEquals(row[9], rs.getTime(10));
        assertEquals(row[9], rs.getTime("t"));

        assertEquals(row[10], rs.getTimestamp(11));
        assertEquals(row[10], rs.getTimestamp("ts"));

        assertFalse(rs.next());
    }


    private DataColumn column(String name, int type) {
        return DataColumn.DataColumnRole.DATA.create(SCHEMA, TABLE, name, name).withType(type);
    }
}