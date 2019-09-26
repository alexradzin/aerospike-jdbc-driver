package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.query.JoinHolder;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
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
import java.util.Optional;

import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.String.format;
import static java.util.Optional.ofNullable;

public class JoinedResultSet implements ResultSet, ResultSetAdaptor, IndexToLabelResultSet, SimpleWrapper {
    private final ResultSet resultSet;
    private final List<JoinHolder> joinHolders;
    private final List<ResultSet> resultSets = new ArrayList<>();
    private boolean moveMainResultSet = true;
    private boolean hasMore = true;
    private DataColumnBasedResultSetMetaData metadata;

    private boolean wasNull = false;
    private int row = 0;


    public JoinedResultSet(ResultSet resultSet, List<JoinHolder> joinHolders) {
        this.resultSet = resultSet;
        this.joinHolders = joinHolders;
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


    @Override
    public String getString(String columnLabel) throws SQLException {
        return get(columnLabel, String.class).orElse(null);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Object result = get(columnLabel, Object.class).orElse(false);
        if (result instanceof Boolean) {
            return (Boolean)result;
        }
        if (result instanceof Number) {
            return ((Number)result).longValue() != 0;
        }
        throw new  ClassCastException(format("Cannot class %s to boolean", result.getClass()));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return get(columnLabel, byte.class).orElse((byte)0);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return get(columnLabel, short.class).orElse((short)0);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return get(columnLabel, int.class).orElse(0);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return get(columnLabel, long.class).orElse(0L);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return get(columnLabel, float.class).orElse(0.0f);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return get(columnLabel, double.class).orElse(0.0);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return get(columnLabel, BigDecimal.class).map(n -> n.setScale(scale, RoundingMode.FLOOR)).orElse(null);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return get(columnLabel, byte[].class).orElse(null);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return get(columnLabel, Date.class).orElse(null);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return get(columnLabel, Time.class).orElse(null);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return get(columnLabel, Timestamp.class).orElse(null);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return get(columnLabel, InputStream.class).orElse(null);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return get(columnLabel, InputStream.class).orElse(null);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return get(columnLabel, InputStream.class).orElse(null);
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
                    metadata.updateData(jh.getMetaDataSupplier().get());
                }
            }
        }
        return metadata;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return get(columnLabel, Object.class);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return 0;
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return get(columnLabel, Reader.class).orElse(null);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return get(columnLabel, BigDecimal.class).orElse(null);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

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
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return row;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return false;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return false;
    }

    @Override
    public Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return get(columnLabel, Blob.class).orElse(null);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return get(columnLabel, Clob.class).orElse(null);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return get(columnLabel, Array.class).orElse(null);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return get(columnLabel, URL.class).orElse(null);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return null;
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
        return get(columnLabel, NClob.class).orElse(null);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return get(columnLabel, Reader.class).orElse(null);
    }


    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return (T)getObject(columnLabel);
    }

    private <T> Optional<T> get(String alias, Class<T> type) throws SQLException {
        if (columnTags(resultSet.getMetaData()).contains(alias)) {
            Object value = resultSet.getObject(alias);
            // Metadata of the main result set contains all fields including those that in fact are retrieved from
            // joined tables. So, we have to perform null check and try to retrieve the data from other result sets
            // if it is null here.
            if (value != null) {
                Optional<T> res = ofNullable(cast(value, type));
                wasNull = !res.isPresent();
                return res;
            }
        }

        for (ResultSet rs : resultSets) {
            if (columnTags(rs.getMetaData()).contains(alias)) {
                Optional<T> res = ofNullable(cast(rs.getObject(alias), type));
                wasNull = !res.isPresent();
                return res;
            }
        }
        wasNull = true;
        return Optional.empty();

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
