package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class DataColumnTest {
    @Test
    void same() {
        DataColumn c = DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab");
        assertEquals(c, c);
    }

    @Test
    void equal() {
        DataColumn c1 = DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab");
        DataColumn c2 = DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab");
        assertEquals(c1, c2);
        assertEquals(c2, c1);
    }

    @Test
    void wrongType() {
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab"), "text");
    }

    @Test
    void notEqual() {
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab"), DataColumn.DataColumnRole.HIDDEN.create("cat", "tab", "col", "lab"));
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat1", "tab", "col", "lab"), DataColumn.DataColumnRole.DATA.create("cat2", "tab", "col", "lab"));
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab1", "col", "lab"), DataColumn.DataColumnRole.DATA.create("cat", "tab2", "col", "lab"));
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col1", "lab"), DataColumn.DataColumnRole.DATA.create("cat", "tab", "col2", "lab"));
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab1"), DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab2"));
        assertNotEquals(DataColumn.DataColumnRole.EXPRESSION.create("cat", "tab", "1+1", "lab"), DataColumn.DataColumnRole.DATA.create("cat", "tab", "2*2", "lab"));

        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab"), null);
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col", "lab"), "");
        assertNotEquals(DataColumn.DataColumnRole.DATA.create("cat", "tab", "col1", "lab1").withType(Types.INTEGER), DataColumn.DataColumnRole.DATA.create("cat", "tab", "col1", "lab1").withType(Types.BIGINT));

    }
}