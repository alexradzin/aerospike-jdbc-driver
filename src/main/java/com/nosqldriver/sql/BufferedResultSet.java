package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class BufferedResultSet implements ResultSet, DelegatingResultSet, ResultSetAdaptor {
    private final ResultSet rs;
    private final Collection<Map<String, Object>> buffer;
    private Iterator<Map<String, Object>> it;
    private Map<String, Object> current;
    private int row = 0;
    private ResultSetMetaData md;
    private boolean bufferIsFull = false;
    private boolean wasNull = false;

    protected BufferedResultSet(ResultSet rs, Collection<Map<String, Object>> buffer) {
        this.rs = rs;
        this.buffer = buffer;
    }

    @Override
    public boolean next() throws SQLException {
        if (bufferIsFull) {
            if (row < buffer.size()) {
                row++;
                current = it.next();
                return true;
            } else {
                bufferIsFull = false;
                return false;
            }
        }

        while (rs.next()) {
            if(!buffer.add(getData())) {
                break;
            }
        }
        bufferIsFull = true;
        row++;
        absolute(row);
        return !buffer.isEmpty();

    }

    @Override
    public void close() throws SQLException {
        rs.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return md == null ? md = rs.getMetaData() : md;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(getMetaData().getColumnLabel(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        Object value =  current.get(columnLabel);
        wasNull = value == null;
        return value;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return row == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return row == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {
        row = -1;
        it = buffer.iterator();
    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        row = 0;
        it = buffer.iterator();
        if (it.hasNext()) {
            row = 1;
            current = it.next();
            return true;
        }
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        it = buffer.iterator();
        // TODO: lists and navigable sets can do this much better.
        int i = 0;
        for (; i < row && it.hasNext(); i++) {
            current = it.next();
        }
        return i == row;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public boolean previous() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_SCROLL_SENSITIVE;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return rs.isClosed();
    }



    private Map<String, Object> getData() throws SQLException {
        int n = getMetaData().getColumnCount();
        Map<String, Object> row = new LinkedHashMap<String, Object>(n);
        for (int i = 1; i <= n ; i++) {
            row.put(getMetaData().getColumnLabel(i), rs.getObject(i)); // TODO: should catalog and table names be part of key?
        }
        return row;
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        return rs.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        rs.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return rs.getCursorName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLException("Cannot unwrap " + iface, e);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
