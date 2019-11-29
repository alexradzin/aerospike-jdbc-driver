package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;

public class BufferedResultSet implements ResultSet, DelegatingResultSet, ResultSetAdaptor, SimpleWrapper {
    private final ResultSet rs;
    private final Collection<Map<String, Object>> buffer;
    private final int fetchSize;
    private Iterator<Map<String, Object>> it;
    private Map<String, Object> current;
    private int row = 0;
    private ResultSetMetaData md;
    private boolean bufferIsFull = false;
    private boolean wasNull = false;
    private boolean afterLast = false;
    private boolean atLast = false;

    protected BufferedResultSet(ResultSet rs, Collection<Map<String, Object>> buffer, int fetchSize) {
        this.rs = rs;
        this.buffer = buffer;
        this.fetchSize = fetchSize;
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
                afterLast = true;
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
        boolean result = !buffer.isEmpty();
        if (!result) {
            afterLast = true;
        }
        return result;

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
        Object value = current == null ? null : current.get(columnLabel);
        wasNull = value == null;
        return value;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return row == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return row == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        return atLast;
    }

    @Override
    public void beforeFirst() throws SQLException {
        row = 0;
        it = buffer.iterator();
    }

    @Override
    public void afterLast() throws SQLException {
        while(next());
        afterLast = true;
        atLast = false;
    }

    @Override
    public boolean first() throws SQLException {
        atLast = false;
        if(buffer.isEmpty()) {
            return next();
        }

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
        while(next());

        Map<String, Object> lastRecord = null;
        do {
            lastRecord = current;
        } while(next());
        current = lastRecord;
        afterLast = current == null;
        atLast = true;
        return current != null;
    }

    @Override
    public int getRow() throws SQLException {
        return afterLast ? 0 : row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        it = buffer.iterator();
        atLast = false;
        return relative(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        int i = 0;
        for (; i < rows && it.hasNext(); i++) {
            current = it.next();
            atLast = false;
        }
        atLast = !it.hasNext();
        return i == rows;
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        rs.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return rs.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows != fetchSize) {
            throw new SQLException(format("Fetch size cannot be changed at runtime. The current fetch size is %d", buffer.size()));
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getType() throws SQLException {
        return rs.getType();
    }

    @Override
    public int getConcurrency() throws SQLException {
        return rs.getConcurrency();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return rs.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        return rs.getHoldability();
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
}
