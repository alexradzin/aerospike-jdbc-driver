package com.nosqldriver.sql;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
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
import java.sql.SQLWarning;
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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static com.nosqldriver.sql.SqlLiterals.sqlTypeNames;
import static com.nosqldriver.sql.SqlLiterals.sqlTypes;
import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.String.format;
import static java.sql.Types.JAVA_OBJECT;
import static java.util.Optional.ofNullable;

public abstract class BaseSchemalessResultSet<R> implements ResultSet, ResultSetAdaptor, SimpleWrapper {
    protected final String schema;
    protected final String table;
    protected final List<DataColumn> columns;
    private final Supplier<R> anyRecordSupplier;
    private boolean wasNull = false;
    private volatile SQLWarning sqlWarning;
    private volatile int index = 0;
    private volatile boolean done = false;
    private volatile boolean closed = false;
    private boolean firstNextWasCalled = false;

    private boolean beforeFirst = true;
    private boolean afterLast = false;

    private static final Map<Class, Function<Object[], Object[]>> collectionTransformers = new HashMap<>();
    static {
        collectionTransformers.put(byte[].class, values -> Arrays.stream(values).map(e -> new ByteArrayBlob((byte[])e)).toArray());
    }



    protected BaseSchemalessResultSet(String schema, String table, List<DataColumn> columns, Supplier<R> anyRecordSupplier) {
        this.schema = schema;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
        this.anyRecordSupplier = anyRecordSupplier;
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
    public String getString(int columnIndex) throws SQLException {
        return getString(getName(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getBoolean(getName(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getByte(getName(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getShort(getName(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getInt(getName(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getLong(getName(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getFloat(getName(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getDouble(getName(columnIndex));
    }

    @Override
    @Deprecated//(since="1.2")
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getBigDecimal(getName(columnIndex));
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getBytes(getName(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(getName(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(getName(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(getName(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getAsciiStream(getName(columnIndex));
    }

    @Override
    @Deprecated//(since="1.2")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getUnicodeStream(getName(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getBinaryStream(getName(columnIndex));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(getRecord(), columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(getRecord(), columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(getRecord(), columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(getRecord(), columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(getRecord(), columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(getRecord(), columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(getRecord(), columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(getRecord(), columnLabel);
    }

    @Override
    @Deprecated//(since="1.2")
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal(String, scale) is deprecated in java.sql API and is not supported here");
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return (byte[])getValue(getRecord(), columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getValue(columnLabel, v -> v instanceof Date ? (Date)v : new Date((Long)v));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getValue(columnLabel, v -> v instanceof Time ? (Time)v : new Time((Long)v));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getValue(columnLabel, v -> v instanceof Timestamp ? (Timestamp)v : new Timestamp((Long)v));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getUnicodeStream(columnLabel);
    }

    @Override
    @Deprecated//(since="1.2")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getBinaryStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, v -> new ByteArrayInputStream((byte[])v));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        String[] names = columns.stream().map(DataColumn::getName).toArray(String[]::new);
        int[] types = new int[names.length];
        boolean shouldDiscover = names.length == 0;
        for (int i = 0; i < types.length; i++) {
            int type = columns.get(i).getType();
            types[i] = type;
            if (type == 0) {
                shouldDiscover = true;
            }
        }

        if (!shouldDiscover) {
            return new DataColumnBasedResultSetMetaData(columns);
        }


        R sampleRecord = getSampleRecord();
        if (sampleRecord == null) {
            return new DataColumnBasedResultSetMetaData(schema, table);
        }

        Map<String, Object> data = getData(sampleRecord);

        if (columns.stream().allMatch(c -> HIDDEN.equals(c.getRole()))) {
            DataColumnBasedResultSetMetaData md = new DataColumnBasedResultSetMetaData(data.entrySet().stream().map(e -> DATA.create(schema, table, e.getKey(), e.getKey()).withType(e.getValue() != null ? sqlTypes.get(e.getValue().getClass()) : 0)).collect(Collectors.toList()));
            md.setDiscovered(true);
            return md;
        }

        columns.stream().filter(c -> c.getType() == 0).filter(c -> data.containsKey(c.getName())).forEach(c ->  Optional.ofNullable(sqlTypes.getOrDefault(getClassOf(data.get(c.getName())), JAVA_OBJECT)).map(c::withType));
        return new DataColumnBasedResultSetMetaData(columns);
    }


    private Class<?> getClassOf(Object obj) {
        return obj == null ? null : obj.getClass();
    }


    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getObject(getName(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return ofNullable(getRecord()).map(r -> getValue(r, columnLabel)).orElse(null);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return IntStream.range(0, columns.size())
                .filter(i -> columnLabel.equals(columns.get(i).getName()))
                .findFirst().orElseThrow(() -> new SQLException(format("Column %s does not exist", columnLabel)));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(getName(columnIndex));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return new StringReader(getString(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(getName(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return new BigDecimal(getDouble(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return beforeFirst;
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
        return done;
    }

    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getRow() throws SQLException {
        return index;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            sqlWarning.setNextWarning(new SQLWarning(format("Attempt to set unsupported fetch direction %d. Only FETCH_FORWARD=%d is supported. The value is ignored.", direction, FETCH_FORWARD)));
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows != 1) {
            throw new SQLException("Fetch size other than 1 is not supported right now");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
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
    public Ref getRef(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return new ByteArrayBlob(getBytes(columnIndex));
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return new StringClob(getString(columnIndex));
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
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
        return new ByteArrayBlob(getBytes(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return new StringClob(getString(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        Object obj = getObject(columnLabel);
        if (obj == null) {
            return null;
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
        throw new ClassCastException(format("Cannot cast value of column %s to array", columnLabel));
    }

    private Class<?> getComponentType(Collection<Object> it) {
        Set<Class> types = it.stream().filter(Objects::nonNull).map(Object::getClass).collect(Collectors.toSet());
        return types.size() == 1 ? types.iterator().next() : Object.class; //TODO discover the nearest common parent if size > 1
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(getName(columnIndex), cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        long epoch = getLong(columnLabel);
        cal.setTime(new java.util.Date(epoch));
        return new Date(cal.getTime().getTime());
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(getName(columnIndex), cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        long epoch = getLong(columnLabel);
        cal.setTime(new java.util.Date(epoch));
        return new Time(cal.getTime().getTime());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(getName(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        long epoch = getLong(columnLabel);
        cal.setTime(new java.util.Date(epoch));
        return new Timestamp(cal.getTime().getTime());
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getURL(getName(columnIndex));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        try {
            return new URL(getString(columnLabel));
        } catch (MalformedURLException e) {
            throw new SQLException(e);
        }
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
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("Type is null");
        }
        Object value = getValue(getRecord(), columnLabel);
        if (value == null) {
            return null;
        }
        if (type.isAssignableFrom(value.getClass())) {
            return (T)value;
        }

        return cast(value, type);
    }

    private String getName(int index) throws SQLException {
        return getMetaData().getColumnName(index);
    }

    private <T> T getValue(String columnLabel, Function<Object, T> mapper) {
        return ofNullable(getValue(getRecord(), columnLabel)).map(mapper).orElse(null);
    }


    protected void assertClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Result set is closed");
        }
    }

    protected abstract R getRecord();
    protected abstract Map<String, Object> getData(R record);

    @Override
    public boolean next() throws SQLException {
        if (firstNextWasCalled && index == 1) {
            firstNextWasCalled = false;
            beforeFirst = false;
            return true;
        }
        boolean result = moveToNext();
        if (result) {
            index++;
        } else {
            done = true;
            afterLast = true;
        }
        return result;
    }


    protected R getSampleRecord() {
        if (index > 0) {
            return getRecord();
        }

        try {
            if (next()) {
                firstNextWasCalled = true;
                return getRecord();
            }
        } catch (SQLException e) {
            return null;
        }

        return anyRecordSupplier.get();
    }

    protected abstract boolean moveToNext();

    protected abstract Object getValue(R record, String label);
    protected abstract String getString(R record, String label);
    protected abstract boolean getBoolean(R record, String label);

    protected abstract byte getByte(R record, String label);
    protected abstract short getShort(R record, String label);
    protected abstract int getInt(R record, String label);
    protected abstract long getLong(R record, String label);
    protected abstract float getFloat(R record, String label);
    protected abstract double getDouble(R record, String label);
}
