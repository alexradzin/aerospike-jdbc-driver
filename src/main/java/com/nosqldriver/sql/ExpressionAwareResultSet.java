package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.SneakyThrower;
import com.nosqldriver.util.ThrowingFunction;
import com.nosqldriver.util.ThrowingSupplier;
import com.nosqldriver.util.ValueExtractor;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.nosqldriver.sql.TypeTransformer.cast;
import static com.nosqldriver.util.ScriptEngineWrapper.EMPTY_COLUMN_PLACEHOLDER;
import static java.lang.System.currentTimeMillis;
import static java.sql.Types.OTHER;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

@VisibleForPackage
class ExpressionAwareResultSet extends ResultSetWrapper {
    private final ScriptEngine engine;
    private final ResultSet rs;
    private final Map<String, String> aliasToEval;
    private boolean wasNull = false;
    private volatile ResultSetMetaData metaData;

    @VisibleForPackage
    ExpressionAwareResultSet(ResultSet rs, FunctionManager functionManager, DriverPolicy driverPolicy, List<DataColumn> columns, boolean indexByName) {
        super(rs, columns, indexByName);
        //TODO: store DataColumn in aliasToEval, call it alias to Expression
        aliasToEval = columns.stream().filter(c -> DataColumn.DataColumnRole.EXPRESSION.equals(c.getRole())).filter(c -> c.getLabel() != null).collect(toMap(DataColumn::getLabel, DataColumn::getExpression));
        engine = new ScriptEngineFactory(functionManager, driverPolicy).getEngine();
        this.rs = rs;
    }


    @Override
    public String getString(int columnIndex) throws SQLException {
        return getValue(columnIndex, String.class, () -> ExpressionAwareResultSet.super.getString(columnIndex));
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getValue(columnIndex, String.class, () -> ExpressionAwareResultSet.super.getString(columnIndex));
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return (boolean)getValue(
                columnIndex,
                Object.class,
                v -> TypeTransformer.cast(v, boolean.class),
                () -> ExpressionAwareResultSet.super.getBoolean(columnIndex));
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getValue(columnIndex, Byte.class, () -> ExpressionAwareResultSet.super.getByte(columnIndex));
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getValue(columnIndex, Short.class, () -> ExpressionAwareResultSet.super.getShort(columnIndex));
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getValue(columnIndex, Integer.class, () -> ExpressionAwareResultSet.super.getInt(columnIndex));
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getValue(columnIndex, Long.class, () -> ExpressionAwareResultSet.super.getLong(columnIndex));
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getValue(columnIndex, Float.class, () -> ExpressionAwareResultSet.super.getFloat(columnIndex));
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getValue(columnIndex, Double.class, () -> ExpressionAwareResultSet.super.getDouble(columnIndex));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getValue(columnIndex, BigDecimal.class, d -> d.setScale(scale, RoundingMode.FLOOR), () -> ExpressionAwareResultSet.super.getBigDecimal(columnIndex, scale));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getValue(columnIndex, BigDecimal.class, () -> ExpressionAwareResultSet.super.getBigDecimal(columnIndex));
    }


    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getValue(columnIndex, byte[].class, () -> ExpressionAwareResultSet.super.getBytes(columnIndex));
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getValue(columnIndex, Date.class, () -> ExpressionAwareResultSet.super.getDate(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getValue(columnIndex, Time.class, () -> ExpressionAwareResultSet.super.getTime(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getValue(columnIndex, Timestamp.class, () -> ExpressionAwareResultSet.super.getTimestamp(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getValue(columnIndex, InputStream.class, () -> ExpressionAwareResultSet.super.getAsciiStream(columnIndex));
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getValue(columnIndex, InputStream.class, () -> ExpressionAwareResultSet.super.getUnicodeStream(columnIndex));
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getValue(columnIndex, InputStream.class, () -> ExpressionAwareResultSet.super.getBinaryStream(columnIndex));
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getValue(columnIndex, Reader.class, () -> ExpressionAwareResultSet.super.getCharacterStream(columnIndex));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }


    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValue(columnLabel, String.class, () -> ExpressionAwareResultSet.super.getString(columnLabel));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getValue(columnLabel, String.class, () -> ExpressionAwareResultSet.super.getString(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return (boolean)getValue(
                columnLabel,
                Object.class,
                v -> cast(v, boolean.class),
                () -> ExpressionAwareResultSet.super.getBoolean(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getValue(columnLabel, Byte.class, () -> ExpressionAwareResultSet.super.getByte(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getValue(columnLabel, Short.class, () -> ExpressionAwareResultSet.super.getShort(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getValue(columnLabel, Integer.class, () -> ExpressionAwareResultSet.super.getInt(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getValue(columnLabel, Long.class, () -> ExpressionAwareResultSet.super.getLong(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValue(columnLabel, Float.class, () -> ExpressionAwareResultSet.super.getFloat(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValue(columnLabel, Double.class, () -> ExpressionAwareResultSet.super.getDouble(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValue(columnLabel, BigDecimal.class, d -> d.setScale(scale, RoundingMode.FLOOR), () -> ExpressionAwareResultSet.super.getBigDecimal(columnLabel, scale));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getValue(columnLabel, byte[].class, () -> ExpressionAwareResultSet.super.getBytes(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getValue(columnLabel, Date.class, () -> ExpressionAwareResultSet.super.getDate(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getValue(columnLabel, Time.class, () -> ExpressionAwareResultSet.super.getTime(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getValue(columnLabel, Timestamp.class, () -> ExpressionAwareResultSet.super.getTimestamp(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, InputStream.class, () -> ExpressionAwareResultSet.super.getAsciiStream(columnLabel));
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, InputStream.class, () -> ExpressionAwareResultSet.super.getUnicodeStream(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getValue(columnLabel, InputStream.class, () -> ExpressionAwareResultSet.super.getBinaryStream(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue(columnIndex, Object.class, () -> ExpressionAwareResultSet.super.getObject(columnIndex));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue(columnLabel, Object.class, () -> ExpressionAwareResultSet.super.getObject(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        if (metaData != null) {
            return metaData;
        }
        DataColumnBasedResultSetMetaData md = (DataColumnBasedResultSetMetaData)rs.getMetaData();
        List<DataColumn> dataColumns = md.getColumns().stream().filter(c -> !DataColumn.DataColumnRole.EXPRESSION.equals(c.getRole())).collect(toList());
        List<DataColumn> expressions = md.getColumns().stream().filter(c -> DataColumn.DataColumnRole.EXPRESSION.equals(c.getRole())).filter(c -> c.getType() == 0).collect(toList());
        if (!expressions.isEmpty()) {
            Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            Collection<String> bound = bind(rs, columns, bindings);

            for (DataColumn column : dataColumns) {
                String name = column.getName();
                if (name != null && !bound.contains(name)) {
                    int type = column.getType();
                    Object value = null;
                    switch (type) {
                        case Types.BIGINT:
                        case Types.INTEGER:
                        case Types.SMALLINT:
                            value = currentTimeMillis() / 10000;
                            break;
                        case Types.DOUBLE:
                        case Types.FLOAT:
                            value = Math.PI * Math.E;
                            break;
                        case Types.VARCHAR:
                        case Types.LONGNVARCHAR:
                        case Types.CLOB:
                            value = "";
                            break;
                        case Types.OTHER:
                        case Types.JAVA_OBJECT:
                            value = new Object();
                            break;
                        case Types.BLOB:
                            value = new byte[0];
                        default:
                            break; // do nothing
                    }
                    if (value != null) {
                        bindings.put(bindingName(name), value);
                    }
                }
            }


            for (DataColumn ec : expressions) {
                try {
                    Object result = eval(ec.getExpression());
                    if (result != null) {
                        ec.withType(SqlLiterals.sqlTypes.getOrDefault(TypeTransformer.getMinimalType(result, Integer.class), OTHER));
                    }
                } catch (Exception e) {
                    // ignore when discovering metadata
                }
            }
        }
        metaData = md;
        return md;
    }

    private Collection<String> bind(ResultSet rs, Collection<DataColumn> columns, Bindings bindings) {
        Collection<String> bound = new HashSet<>();

        columns.stream().filter(c -> !DataColumn.DataColumnRole.EXPRESSION.equals(c.getRole())).map(DataColumn::getName).forEach(name -> {
            try {
                String bindingName = bindingName(name);
                bindings.put(bindingName, rs.getObject(name));
                bound.add(bindingName);
            } catch (SQLException e) {
                // ignore exception thrown by specific field
            }
        });
        return bound;
    }


    private Object eval(String expr) {
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        Collection<String> bound = bind(rs, columns, bindings);
        try {
            return engine.eval(expr);
        } catch (ScriptException | RuntimeException e) {
            return SneakyThrower.sneakyThrow(e instanceof RuntimeException && e.getCause() instanceof SQLException ? e.getCause() : new SQLException(e));
        } finally {
            bound.forEach(bindings::remove);
        }
    }

    private String getEval(int index) {
        return index <= columns.size() ? ofNullable(columns.get(index - 1)).map(DataColumn::getExpression).orElse(null) : null;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    private <T> T wasNull(T value) {
        wasNull = value == null;
        return value;
    }

    private <T> T getValue(int columnIndex, Class<T> type, ThrowingSupplier<T, SQLException> superGetter) throws SQLException {
        return getValue(columnIndex, type, v -> v, superGetter);
    }


    private <T> T getValue(int columnIndex, Class<T> type, ThrowingFunction<T, T, SQLException> transformer, ThrowingSupplier<T, SQLException> superGetter) throws SQLException {
        DataColumnBasedResultSetMetaData md = (DataColumnBasedResultSetMetaData)getMetaData();
        return getValueUsingExpression(getEval(columnIndex), type, v -> v, transformer, superGetter);
    }

    private <T> T getValue(String columnLabel, Class<T> type, ThrowingSupplier<T, SQLException> superGetter) throws SQLException {
        return getValue(columnLabel, type, v -> v, superGetter);
    }

    private <T> T getValue(String columnLabel, Class<T> type, ThrowingFunction<T, T, SQLException> transformer, ThrowingSupplier<T, SQLException> superGetter) throws SQLException {
        String alias = columnLabel.replaceFirst("\\[.*", "");
        Function<Object, Object> transformer1 = alias.equals(columnLabel) ? v -> v : t -> new ValueExtractor().getValue(t, columnLabel.substring(alias.length()));
        return getValueUsingExpression(aliasToEval.get(alias), type, transformer1, transformer, superGetter);
    }

    private <T> T getValueUsingExpression(String expr, Class<T> type, Function<Object, Object> transformer1, ThrowingFunction<T, T, SQLException> transformer, ThrowingSupplier<T, SQLException> superGetter) throws SQLException {
        try {
            T value = expr != null ? transformer.apply(cast(transformer1.apply(eval(expr)), type)) : null;
            return getValue(Optional.ofNullable(value), superGetter);
        } catch (ClassCastException e) {
            throw new SQLException(e);
        }
    }

    private <T> T getValue(Optional<T> value, ThrowingSupplier<T, SQLException> superGetter) throws SQLException {
        return wasNull(value.isPresent() ? value.get() : superGetter.get());
    }

    private String bindingName(String name) {
        return "".equals(name) ? EMPTY_COLUMN_PLACEHOLDER : name;
    }
}
