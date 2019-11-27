package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.query.JoinHolder;
import com.nosqldriver.util.ThrowingFunction;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JoinedResultSet implements ResultSet, ResultSetAdaptor, IndexToLabelResultSet, SimpleWrapper {
    private final ResultSet resultSet;
    private final List<JoinHolder> joinHolders;
    private final List<ResultSet> resultSets = new ArrayList<>();
    private boolean moveMainResultSet = true;
    private boolean hasMore = true;
    private DataColumnBasedResultSetMetaData metadata;

    private boolean wasNull = false;
    private int row = 0;
    private final boolean pureLeftJoin;


    public JoinedResultSet(ResultSet resultSet, List<JoinHolder> joinHolders) {
        this.resultSet = resultSet;
        this.joinHolders = joinHolders;
        pureLeftJoin = !joinHolders.stream().anyMatch(JoinHolder::isSkipIfMissing);
    }

    @Override
    public boolean next() throws SQLException {
        if (moveMainResultSet) {
            SUPER:
            while (resultSet.next()) {
                resultSets.clear();
                moveMainResultSet = false;
                for (JoinHolder jh : joinHolders) {
                    ResultSet rs = jh.getResultSetRetriever().apply(resultSet);
                    boolean hasNext = rs != null && rs.next();
                    if (jh.isSkipIfMissing() && !hasNext) {
                        continue SUPER;
                    }
                    resultSets.add(rs);
                }
                moveMainResultSet = false;
                hasMore = true;
                row++;
                return true;
            }
            moveMainResultSet = false;
            hasMore = false;
            return false;
        } else if(hasMore) {
            boolean allDone = true;
            for (int i = 0; i < joinHolders.size(); i++) {
                ResultSet rs = resultSets.get(i);
                boolean hasNext = rs.next();
                if (hasNext) {
                    allDone = false;
                }
                if (!hasNext && joinHolders.get(i).isSkipIfMissing()) {
                    moveMainResultSet = true;
                    break;
                }
            }
            moveMainResultSet = allDone;
            if (!moveMainResultSet) {
                row++;
                return true;
            }
            return next();
        } else {
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        resultSet.close();
        for (ResultSet rs : resultSets) {
            rs.close();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    private <T> T wasNull(T value) {
        wasNull = value == null;
        return value;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getString(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getBoolean(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getByte(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getShort(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getInt(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getLong(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getFloat(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getDouble(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValue(columnLabel, rs -> rs.getBigDecimal(columnLabel, scale));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getBytes(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getDate(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getTime(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getTimestamp(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getAsciiStream(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getUnicodeStream(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getBinaryStream(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return resultSet.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        resultSet.clearWarnings();
    }

    @Override
    public String getCursorName() throws SQLException {
        return resultSet.getCursorName();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (metadata == null) {
            metadata = (DataColumnBasedResultSetMetaData) resultSet.getMetaData();
            if (metadata.isDiscovered()) {
                List<DataColumn> allColumns = new ArrayList<>(metadata.getColumns());
                for (JoinHolder jh : joinHolders) {
                    allColumns.addAll(((DataColumnBasedResultSetMetaData)jh.getMetaDataSupplier().get()).getColumns());
                }
                metadata = new DataColumnBasedResultSetMetaData(allColumns);
            } else {
                for (JoinHolder jh : joinHolders) {
                    metadata = metadata.updateData(jh.getMetaDataSupplier().get());
                }
            }
        }
        return metadata;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getObject(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return DelegatingResultSet.findColumn(getMetaData(), columnLabel);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getCharacterStream(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getBigDecimal(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return resultSet.isBeforeFirst();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return resultSet.isAfterLast();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return resultSet.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        if (pureLeftJoin) {
            return resultSet.isLast();
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        resultSet.beforeFirst();
    }

    @Override
    public void afterLast() throws SQLException {
        resultSet.afterLast();
    }

    @Override
    public boolean first() throws SQLException {
        if (row == 0) {
            return next();
        }
        if (row == 1) {
            return true;
        }
        throw new SQLException("Cannot got backwards on result set type TYPE_FORWARD_ONLY");
    }

    @Override
    public boolean last() throws SQLException {
        if (pureLeftJoin) {
            return resultSet.last();
        }
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return resultSet.absolute(row);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return resultSet.relative(rows);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return resultSet.getStatement();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getValue(columnLabel, rs -> rs.getObject(columnLabel, map));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getRef(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getBlob(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getClob(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getArray(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getValue(columnLabel, rs -> rs.getDate(columnLabel, cal));
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getValue(columnLabel, rs -> rs.getTime(columnLabel, cal));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getValue(columnLabel, rs -> rs.getTimestamp(columnLabel, cal));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getURL(columnLabel));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return findResultSet(columnLabel).getRowId(columnLabel);
    }

    @Override
    public int getHoldability() throws SQLException {
        return resultSet.getHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return resultSet.isClosed();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getNClob(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getSQLXML(columnLabel));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getNString(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, rs -> rs.getNCharacterStream(columnLabel));
    }


    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getValue(columnLabel, rs -> rs.getObject(columnLabel, type));
    }

    private <T> T getValue(String columnLabel, ThrowingFunction<ResultSet, T, SQLException> getter) throws SQLException {
        ResultSet rs = findResultSet(columnLabel);
        return wasNull(rs.isBeforeFirst() ? null : getter.apply(rs));
    }

    private ResultSet findResultSet(String alias) throws SQLException {
        if (columnTags(resultSet.getMetaData()).contains(alias)) {
            Object value = resultSet.getObject(alias);
            // Metadata of the main result set contains all fields including those that in fact are retrieved from
            // joined tables. So, we have to perform null check and try to retrieve the data from other result sets
            // if it is null here.
            if (value != null) {
                return resultSet;
            }
        }

        for (ResultSet rs : resultSets) {
            if (columnTags(rs.getMetaData()).contains(alias)) {
                return rs;
            }
        }
        return resultSet;

    }

    private Collection<String> columnTags(ResultSetMetaData md) throws SQLException {
        int columnCount = md.getColumnCount();
        Collection<String> labels = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            String label = md.getColumnLabel(i + 1);
            String tag = label != null ? label : md.getColumnName(i + 1);
            labels.add(tag);
        }
        return labels;
    }
}
