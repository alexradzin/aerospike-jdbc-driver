package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.sql.SQLWarning;

import static org.junit.jupiter.api.Assertions.*;

class WarningsHolderTest {
    @Test
    void init() throws SQLException {
        assertNull(new WarningsHolder().getWarnings());
    }

    @Test
    void oneWarning() throws SQLException {
        WarningsHolder holder = new WarningsHolder();
        assertNull(holder.getWarnings());
        holder.addWarning("Something is bad");
        SQLWarning w = holder.getWarnings();
        assertNotNull(w);
        assertEquals("Something is bad", w.getMessage());
        assertNull(w.getNextWarning());

        holder.clearWarnings();
        assertNull(holder.getWarnings());
    }

    @Test
    void twoWarnings() throws SQLException {
        WarningsHolder holder = new WarningsHolder();
        assertNull(holder.getWarnings());
        holder.addWarning("Something is bad");
        holder.addWarning("Even worse");
        SQLWarning w = holder.getWarnings();
        assertNotNull(w);
        assertEquals("Something is bad", w.getMessage());
        SQLWarning w2 = w.getNextWarning();
        assertNotNull(w2);
        assertEquals("Even worse", w2.getMessage());

        holder.clearWarnings();
        assertNull(holder.getWarnings());
    }
}