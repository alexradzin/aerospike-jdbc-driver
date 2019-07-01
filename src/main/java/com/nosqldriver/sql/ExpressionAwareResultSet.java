package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.InputStream;
import java.math.BigDecimal;
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

import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.System.currentTimeMillis;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.IntStream.range;

@VisibleForPackage
class ExpressionAwareResultSet extends ResultSetWrapper {
    private final ScriptEngine engine;
    private final ResultSet rs;
    private final List<String> evals;
    private final Map<String, String> aliasToEval;

    @VisibleForPackage
    ExpressionAwareResultSet(ResultSet rs, List<String> names, List<String> evals, List<String> aliases) {
        super(rs, names, aliases);
        aliasToEval = range(0, aliases.size()).boxed().filter(i -> evals.size() > i && evals.get(i) != null).collect(toMap(aliases::get, evals::get));
        engine = new JavascriptEngineFactory().getEngine();
        this.rs = rs;
        this.evals = evals;
    }


    @Override
    public String getString(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), String.class) : super.getString(columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), boolean.class) : super.getBoolean(columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), byte.class) : super.getByte(columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), short.class) : super.getShort(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), int.class) : super.getInt(columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), long.class) : super.getLong(columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), float.class) : super.getFloat(columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), double.class) : super.getDouble(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), BigDecimal.class) : super.getBigDecimal(columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), byte[].class) : super.getBytes(columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), Date.class) : super.getDate(columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), Time.class) : super.getTime(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), Timestamp.class) : super.getTimestamp(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), InputStream.class) : super.getAsciiStream(columnIndex);
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), InputStream.class) : super.getUnicodeStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? cast(eval(eval), InputStream.class) : super.getBinaryStream(columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), String.class) : super.getString(columnLabel);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), boolean.class) : super.getBoolean(columnLabel);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), byte.class) : super.getByte(columnLabel);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), short.class) : super.getShort(columnLabel);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), int.class) : super.getInt(columnLabel);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), long.class) : super.getLong(columnLabel);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), float.class) : super.getFloat(columnLabel);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), double.class) : super.getDouble(columnLabel);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), BigDecimal.class) : super.getBigDecimal(columnLabel);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), byte[].class) : super.getBytes(columnLabel);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), Date.class) : super.getDate(columnLabel);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), Time.class) : super.getTime(columnLabel);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), Timestamp.class) : super.getTimestamp(columnLabel);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), InputStream.class) : super.getAsciiStream(columnLabel);
    }

    @Override
    @Deprecated
    @SuppressWarnings("deprecation")
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), InputStream.class) : super.getUnicodeStream(columnLabel);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? cast(eval(eval), InputStream.class) : super.getBinaryStream(columnLabel);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        String eval = getEval(columnIndex);
        return eval != null ? eval(eval) : super.getObject(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        String eval = aliasToEval.get(columnLabel);
        return eval != null ? eval(eval) : super.getObject(columnLabel);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        int[] discoveredExpressionTypes = new int[evals.size()];
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        Collection<String> bound = bind(rs, names, evals, bindings);
        int n = md.getColumnCount();
        for (int i = 0; i < n; i++) {
            String name = md.getColumnName(i + 1);
            if (name != null && !bound.contains(name)) {
                int type = md.getColumnType(i + 1);
                Object value = null;
                switch (type) {
                    case Types.BIGINT: case Types.INTEGER: case Types.SMALLINT: value = currentTimeMillis(); break;
                    case Types.DOUBLE: case Types.FLOAT: value = Math.PI * Math.E; break;
                    case Types.VARCHAR: case Types.LONGNVARCHAR: value = ""; break;
                    default: break; // do nothing
                }
                if (value != null) {
                    bindings.put(name, value);
                }
            }
        }

        boolean typesFound = false;
        for (int i = 0; i < evals.size(); i++) {
            String e = evals.get(i);
            if (e == null) {
                continue;
            }
            Object result = eval(e);
            if (result != null) {
                Integer sqlType = TypeConversion.sqlTypes.get(result.getClass());
                if (sqlType != null) {
                    discoveredExpressionTypes[i] = sqlType;
                    typesFound = true;
                }
            }
        }

        if (typesFound) {
            int[] allTypes = new int[n];
            String[] allNames = new String[n];
            String[] allAliases = new String[n];
            for (int i = 0; i < n; i++) {
                allTypes[i] = discoveredExpressionTypes[i] != 0 ? discoveredExpressionTypes[i] : md.getColumnType(i + 1);
                allNames[i] = md.getColumnName(i + 1);
                allAliases[i] = md.getColumnLabel(i + 1);
            }
            return new SimpleResultSetMetaData(null, md.getSchemaName(1), allNames, allAliases, allTypes);
        }
        return md;
    }

    private Collection<String> bind(ResultSet rs, Collection<String> names, List<String> evals, Bindings bindings) throws SQLException {
        Collection<String> bound = new HashSet<>();
        for (String name : names) {
            try {
                bindings.put(name, rs.getObject(name));
                bound.add(name);
            } catch (SQLException e) {
                // ignore exception thrown by specific field
            }
        }

        ResultSetMetaData md = rs.getMetaData();
        for (int i = 0, j = 1; i < md.getColumnCount(); i++, j++) {
            String name = md.getColumnLabel(j);
            if (evals.size() > i && evals.get(i) == null && !bindings.containsKey(name)) {
                bindings.put(name, rs.getObject(j));
                bound.add(name);
            }
        }
        return bound;
    }

    private Object eval(String eval) throws SQLException {
        Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
        Collection<String> bound = bind(rs, names, evals, bindings);
        try {
            return engine.eval(eval);
        } catch (ScriptException e) {
            throw new SQLException(e);
        } finally {
            bound.forEach(bindings::remove);
        }
    }

    private String getEval(int index) {
        return index <= evals.size() ? evals.get(index - 1) : null;
    }
}
