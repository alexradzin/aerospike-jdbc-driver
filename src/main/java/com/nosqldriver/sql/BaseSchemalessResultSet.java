package com.nosqldriver.sql;

import com.nosqldriver.util.ThrowingBiFunction;
import com.nosqldriver.util.ThrowingSupplier;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
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
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static com.nosqldriver.sql.SqlLiterals.sqlTypeNames;
import static com.nosqldriver.sql.SqlLiterals.sqlTypes;
import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toSet;

// TODO: extend this class from DelegatingResultSet, move all type transformations to TypeTransformer and remove getX() method from here.
public abstract class BaseSchemalessResultSet<R> extends WarningsHolder implements ResultSet, ResultSetAdaptor, SimpleWrapper, IndexToLabelResultSet {
    private final Statement statement;
    protected final String schema;
    protected final String table;
    protected final List<DataColumn> columns;
    private boolean wasNull = false;
    protected volatile int index = 0;
    private volatile boolean closed = false;

    private boolean beforeFirst = true;
    protected boolean afterLast = false;
    private final TypeDiscoverer typeDiscoverer;
    private volatile ResultSetMetaData metadata = null;
    private final List<DataColumn> columnsForMetadata;

    private static final Map<Class, Function<Object[], Object[]>> collectionTransformers = new HashMap<>();
    static {
        collectionTransformers.put(byte[].class, values -> Arrays.stream(values).map(e -> new ByteArrayBlob((byte[])e)).toArray());
    }



    protected BaseSchemalessResultSet(Statement statement, String schema, String table, List<DataColumn> columns, TypeDiscoverer typeDiscoverer) {
        this.statement = statement;
        this.schema = schema;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
        this.typeDiscoverer = typeDiscoverer;

        columnsForMetadata = columns.stream().anyMatch(c -> DATA.equals(c.getRole()) || (EXPRESSION.equals(c.getRole()) && c.getExpression().startsWith("deserialize") )) ?
                columns :
                singletonList(DATA.create(schema, table, "*", "*"));
    }


    @Override
    public void close() throws SQLException {
        closed = true;
    }


    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getString(r, columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getBoolean(r, columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getByte(r, columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return wasNull(getShort(getRecord(), columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getInt(r, columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getLong(r, columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getFloat(r, columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getDouble(r, columnLabel));
    }

    @Override
    @Deprecated//(since="1.2")
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValue(columnLabel, (r, s) -> getBigDecimal(columnLabel).setScale(scale, RoundingMode.FLOOR));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), byte[].class));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), Date.class));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), Time.class));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), Timestamp.class));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getBinaryStream(columnLabel);
    }

    @Override
    @Deprecated//(since="1.2")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getAsciiStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), InputStream.class));
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (metadata == null) {
            metadata = new DataColumnBasedResultSetMetaData(typeDiscoverer.discoverType(columnsForMetadata));
        }
        return metadata;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel, (r, s) -> getValue(r, columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return IntStream.range(0, columns.size())
                .filter(i -> columnLabel.equals(columns.get(i).getName()))
                .map(i -> i + 1)
                .findFirst()
                .orElseThrow(() -> new SQLException(format("Column %s does not exist", columnLabel)));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), Reader.class));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return wasNull(new BigDecimal(getDouble(columnLabel)));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return index == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return index == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void beforeFirst() throws SQLException {
        if (index != 0) {
            throw new SQLFeatureNotSupportedException();
        }
    }

    @Override
    public void afterLast() throws SQLException {
        while(next());
        afterLast = true;
    }

    @Override
    public boolean first() throws SQLException {
        if (index == 0) {
            return next();
        }
        throw new SQLException("Cannot rewind result set");
    }

    @Override
    public boolean last() throws SQLException {
        if (isAfterLast()) {
            return false;
        }
        R lastRecord = null;
        do {
            lastRecord = getRecord();
        } while(next());

        if (lastRecord != null) {
            afterLast = false;
            setCurrentRecord(lastRecord);
            return true;
        }
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return afterLast ? 0 : index;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return relative(row - index);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        ThrowingSupplier<Boolean, SQLException> supplier = rows >=0 ? this::next : this::previous;
        int n = Math.abs(rows);
        for (int i = 0; i < n; i++) {
            if (!supplier.get()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            addWarning(format("Attempt to set unsupported fetch direction %d. Only FETCH_FORWARD=%d is supported. The value is ignored.", direction, FETCH_FORWARD));
        }
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), Blob.class));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getNClob(columnLabel);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return wasNull(toArray(getObject(columnLabel), columnLabel));
    }

    private <T> Array toArray(Object obj, T columnIdentifier) throws SQLException {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Array) {
            return (Array)obj;
        }
        if (obj.getClass().isArray()) {
            return new BasicArray(schema, sqlTypeNames.get(sqlTypes.get(obj.getClass().getComponentType())), (Object[])obj);
        }

        if (obj instanceof Collection) {
            Class type = getComponentType(((Collection<Object>)obj));
            Object[] rawValues = ((Collection)obj).toArray();
            Object[] values = collectionTransformers.getOrDefault(type, a -> a).apply(rawValues);
            return new BasicArray(schema, sqlTypeNames.get(sqlTypes.get(type)), values);
        }
        throw new ClassCastException(format("Cannot cast value of column %s to array", columnIdentifier));
    }



    private Class<?> getComponentType(Collection<Object> it) {
        Set<Class> types = it.stream().filter(Objects::nonNull).map(Object::getClass).collect(toSet());
        return types.size() == 1 ? types.iterator().next() : Object.class; //TODO discover the nearest common parent if size > 1
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return TypeTransformer.getDate(getLong(columnLabel), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return TypeTransformer.getTime(getLong(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return TypeTransformer.getTimestamp(getLong(columnLabel), cal);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        try {
            String spec = getString(columnLabel);
            return wasNull(spec != null ? new URL(spec) : null);
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return wasNull(cast(getValue(getRecord(), columnLabel), NClob.class));
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
        return getCharacterStream(columnLabel);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("Type is null");
        }
        Object value = getValue(getRecord(), columnLabel);
        if (value == null) {
            wasNull = true;
            return null;
        }
        if (type.isAssignableFrom(value.getClass())) {
            return wasNull((T)value);
        }

        return wasNull(cast(value, type));
    }

    /**
     * Retrieves column name instead of label. This "physical" name is used then to get the value from data record that
     * is fetched from the real DB.
     * @param index
     * @return
     * @throws SQLException
     */
    @Override
    public String getColumnLabel(int index) throws SQLException {
        return getMetaData().getColumnName(index);
    }

    protected void assertClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Result set is closed");
        }
    }

    protected abstract R getRecord();

    @Override
    public boolean next() throws SQLException {
        assertClosed();
        boolean result = moveToNext();
        if (result) {
            clearWarnings();
            index++;
        } else {
            afterLast = true;
        }
        return result;
    }


    private <T> T wasNull(T value) {
        wasNull = value == null;
        return value;
    }

    private <V> V getValue(String columnLabel, ThrowingBiFunction<R, String, V, SQLException> getter) throws SQLException {
        if (isBeforeFirst() || isAfterLast()) {
            throw new SQLException("Cursor is not positioned on any row");
        }
        R record = getRecord();
        return wasNull(record == null ? null : getter.apply(record, columnLabel));
    }

    protected abstract boolean moveToNext();

    protected abstract Object getValue(R record, String label);
    protected abstract String getString(R record, String label) throws SQLException;
    protected abstract boolean getBoolean(R record, String label) throws SQLException;

    protected abstract byte getByte(R record, String label) throws SQLException;
    protected abstract short getShort(R record, String label) throws SQLException;
    protected abstract int getInt(R record, String label) throws SQLException;
    protected abstract long getLong(R record, String label) throws SQLException;
    protected abstract float getFloat(R record, String label) throws SQLException;
    protected abstract double getDouble(R record, String label) throws SQLException;

    protected void setCurrentRecord(R r) {
        // default empty implementation
    }
}
