package com.nosqldriver.sql;

import com.nosqldriver.util.SneakyThrower;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.AGGREGATED;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.GROUP;
import static com.nosqldriver.sql.SqlLiterals.sqlTypes;
import static com.nosqldriver.sql.TypeTransformer.commonType;
import static com.nosqldriver.sql.TypeTransformer.getMinimalType;

public class AggregatingResultSet extends WarningsHolder implements DelegatingResultSet, ResultSetAdaptor, SimpleWrapper {
    private static final BiFunction<Object, Object, Object> count = (one, two) -> Optional.ofNullable(one).map(o -> ((Number)o).longValue()).orElse(0L) + ((Number)two).longValue();

    private static final BiFunction<Object, Object, Object> sum = (one, two) -> {
        if (one == null) {
            return two;
        }
        return TypeTransformer.cast(one, Double.class, 0.0) + TypeTransformer.cast(two, Double.class, 0.0);
    };
    private static final BiFunction<Object, Object, Object> sumsqs = (one, two) -> {
        double v1 = Optional.ofNullable(one).map(o -> ((Number)o).doubleValue()).orElse(0.0);
        double v2 = Optional.ofNullable(two).map(t -> ((Number)t).doubleValue()).orElse(0.0);
        return v1 + v2 * v2;
    };
    // sumsqs
    private static final BiFunction<Object, Object, Object> max = (one, two) -> Math.max(value(one), value(two));
    private static final BiFunction<Object, Object, Object> min = (one, two) -> Math.min(value(one), value(two));
    private static final Map<String, BiFunction<Object, Object, Object>> functions = new HashMap<>(); {
        functions.put("count", count);
        functions.put("sum", sum);
        functions.put("sumsqs", sumsqs);
        functions.put("min", min);
        functions.put("max", max);
    }

    private static final Pattern functionPattern = Pattern.compile("^\\s*(\\w+)\\s*\\(");

    private final ResultSet rs;
    private Map<List<Object>, List<Object>> aggregatedResults = new HashMap<>();
    private final List<DataColumn> groupByColumns;
    private final List<DataColumn> aggregatedColumns;
    private final List<BiFunction<Object, Object, Object>> aggregationFunctions;
    private final List<Object> defaultValues; // 0 for count, null for the rest
    private final List<Class> types1 = new ArrayList<>();

    public AggregatingResultSet(ResultSet rs, List<DataColumn> columns) {
        this.rs = rs;
        groupByColumns = columns.stream().filter(c -> GROUP.equals(c.getRole())).collect(Collectors.toList());
        aggregatedColumns = columns.stream().filter(c -> AGGREGATED.equals(c.getRole())).collect(Collectors.toList());
        aggregationFunctions = aggregatedColumns.stream()
                .map(DataColumn::getName)
                .map(functionPattern::matcher)
                .filter(Matcher::find) // TODO: throw exception if pattern is not matched
                .map(m -> m.group(1))
                .map(functions::get) // TODO: throw exception if function is not found
                .collect(Collectors.toList());

        defaultValues = aggregationFunctions.stream().map(f -> count == f ? 0 : null).collect(Collectors.toList());


//        int groupsCount = groupByColumns.size();
//        for (int i = 0; i < groupsCount ; i++) {
//            types.set(i, commonType(types.get(i), getMinimalType(groupByColumns.get(i))));
//        }
//        for (int i = 0; i < aggregatedColumns.size(); i++) {
//            types.set(i, commonType(types.get(i + groupsCount), getMinimalType(aggregatedColumns.get(i))));
//        }

        // map index of column to index in type
        for (int i = 0; i < columns.size(); i++) {

        }




    }

    private void read() throws SQLException {
        while (rs.next()) {
            List<Object> group = groupByColumns.stream().map(c -> getObject(rs, c.getLabel())).collect(Collectors.toList());
            List<Object> aggregatedLine = aggregatedColumns.stream().map(c -> getObject(rs, c.getLabel())).collect(Collectors.toList());
            List<Object> aggregated = aggregate(aggregatedResults.computeIfAbsent(group, objects -> new ArrayList<>(defaultValues)), aggregatedLine);

            int groupsCount = group.size();
            for (int i = 0; i < groupsCount ; i++) {
                updateType(groupByColumns.get(i), group.get(i));
            }
            for (int i = 0; i < aggregated.size(); i++) {
                updateType(aggregatedColumns.get(i), aggregated.get(i));
            }
            aggregatedResults.put(group, aggregated);
        }
    }

    private void updateType(DataColumn column, Object value) {
        Class type = getMinimalType(value);
        Class existingType = SqlLiterals.sqlToJavaTypes.get(column.getType());
        Class newType = commonType(type, existingType);
        column.withType(sqlTypes.get(newType));
    }


    private Object getObject(ResultSet rs, String label) {
        try {
            return rs.getObject(label);
        } catch (SQLException e) {
            return SneakyThrower.sneakyThrow(e);
        }
    }

    private List<Object> aggregate(List<Object> one, List<Object> two) {
        int n = aggregationFunctions.size();
        List<Object> result = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            Object aggregation = aggregationFunctions.get(i).apply(one.get(i), two.get(i));
            result.add(aggregation);
        }
        return result;
    }


    @Override
    public boolean next() throws SQLException {
        return false;
    }

    @Override
    public void close() throws SQLException {
        rs.close();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }

    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return null;
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
        return false;
    }

    @Override
    public boolean last() throws SQLException {
        return false;
    }

    @Override
    public int getRow() throws SQLException {
        return 0;
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
        return rs.getStatement();
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
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
        return rs.isClosed();
    }

    private static Double value(Object v) {
        return Optional.ofNullable(v).map(value -> TypeTransformer.cast(value, Double.class, 0.0)).orElse(0.0);
    }
}

// select first_name, count(age), max(age) from people group by first_name    map(John -> {[123, 50], Bill -> {[321, 60])
// select , count(age), max(age) from people     map(John -> {[123, 50], Bill -> {[321, 60])