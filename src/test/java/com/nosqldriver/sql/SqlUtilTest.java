package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlUtilTest {
    @Test
    void selectUnwrappedTableName() {
        assertEquals("select * from \"test\"", SqlUtil.fix("select * from test"));
    }

    @Test
    void selectUnwrappedTableNameWithAlias() {
        assertEquals("select * from \"test\" tbl", SqlUtil.fix("select * from test tbl"));
    }

    @Test
    void selectWrappedTableName() {
        assertEquals("select * from \"test\"", SqlUtil.fix("select * from \"test\""));
    }

    @Test
    void selectUnwrappedNamespaceAndTableName() {
        assertEquals("select * from \"namespace\".\"table\"", SqlUtil.fix("select * from namespace.table"));
    }

    @Test
    void selectUnwrappedNamespaceAndWrappedTableName() {
        assertEquals("select * from \"namespace\".\"table\"", SqlUtil.fix("select * from namespace.\"table\""));
    }

    @Test
    void selectWrappedNamespaceAndWrappedTableName() {
        assertEquals("select * from \"namespace\".\"table\"", SqlUtil.fix("select * from \"namespace\".\"table\""));
    }

    @Test
    void selectWrappedNamespaceAndUnwrappedTableName() {
        assertEquals("select * from \"namespace\".\"table\"", SqlUtil.fix("select * from \"namespace\".table"));
    }

    @Test
    void insertUnwrappedTableName() {
        String insert = "insert into table (PK, text) values (1, 'hello')";
        assertEquals(insert, SqlUtil.fix(insert));
    }

    @Test
    void selectWithSubSelect() {
        String sql = "select * from (select * from people)";
        assertEquals(sql, SqlUtil.fix(sql));
    }

    // executeQuery: select * from (select * from people)
}