package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleWrapperTest {
    @Test
    void unwrap() throws SQLException {
        ListRecordSet lrs = new ListRecordSet(null, null, null, emptyList(), emptyList());
        successful(lrs, ResultSet.class);
        failing(lrs, Runnable.class);


        BufferedResultSet brs = new BufferedResultSet(null, null, 0);
        successful(brs, ResultSet.class);
        failing(brs, Callable.class);


        SimpleParameterMetaData pmd = new SimpleParameterMetaData(Collections.emptyList());
        successful(pmd, ParameterMetaData.class);
        failing(pmd, Function.class);
    }

    private <T extends Wrapper> void successful(T wrapper, Class<?> iface) throws SQLException {
        assertEquals(wrapper, wrapper.unwrap(iface));
        assertTrue(wrapper.isWrapperFor(iface));
    }

    private <T extends Wrapper> void failing(T wrapper, Class<?> iface) throws SQLException {
        assertThrows(SQLException.class, () -> wrapper.unwrap(iface));
        assertFalse(wrapper.isWrapperFor(iface));
    }

}