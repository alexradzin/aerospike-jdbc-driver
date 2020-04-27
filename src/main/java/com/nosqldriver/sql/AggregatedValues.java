package com.nosqldriver.sql;

import com.nosqldriver.sql.DataColumn.DataColumnRole;
import com.nosqldriver.util.SneakyThrower;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.SqlLiterals.sqlTypes;
import static com.nosqldriver.sql.TypeTransformer.commonType;
import static com.nosqldriver.sql.TypeTransformer.getMinimalType;

public class AggregatedValues {
    private static final BiFunction<Object, Object, Object> count = (one, two) -> cast(Optional.ofNullable(one).map(o -> ((Number)o).longValue()).orElse(0L) + 1);

    private static final BiFunction<Object, Object, Object> sum = (one, two) -> {
        if (one == null) {
            return cast((Number)two);
        }
        return cast(TypeTransformer.cast(one, Double.class, 0.0) + TypeTransformer.cast(two, Double.class, 0.0));
    };

    public static class Avg implements BiFunction<Object, Object, Object> {
        int count = 0;
        @Override
        public Object apply(Object one, Object two) {
            if (one == null) {
                count++;
                return two;
            }
            double sum = TypeTransformer.cast(one, Double.class, 0.0) * count + TypeTransformer.cast(two, Double.class, 0.0);
            count++;
            return sum / count;
        }
    }



    private static final BiFunction<Object, Object, Object> sumsqs = (one, two) -> {
        double v1 = Optional.ofNullable(one).map(o -> ((Number)o).doubleValue()).orElse(0.0);
        double v2 = Optional.ofNullable(two).map(t -> ((Number)t).doubleValue()).orElse(0.0);
        return cast(v1 + v2 * v2);
    };
    // sumsqs
    private static final BiFunction<Object, Object, Object> max = (one, two) -> one == null ? two : cast(Math.max(value(one), value(two)));
    private static final BiFunction<Object, Object, Object> min = (one, two) -> one == null ? two : cast(Math.min(value(one), value(two)));
    private static final Map<String, BiFunction<Object, Object, Object>> functions = new HashMap<>();
    static {
        functions.put("count", count);
        functions.put("sum", sum);
        functions.put("sumsqs", sumsqs);
        functions.put("min", min);
        functions.put("max", max);
    }
    private static final Map<String, Class<? extends BiFunction<Object, Object, Object>>> functionClasses = new HashMap<>();
    static {
        functionClasses.put("avg", Avg.class);
    }

    private static final Pattern functionPattern = Pattern.compile("^\\s*(\\w+)\\s*\\(\\s*(\\w+|\\*)\\s*\\)");

    private final ResultSet rs;
    private final List<DataColumn> columns;
    private Map<List<Object>, List<Object>> aggregatedResults = new HashMap<>();

    private final Map<String, DataColumn> nameToColumn = new HashMap<>();
    private final Map<String, Integer> groupByColumnsIndexes = new LinkedHashMap<>();
    private final Map<String, Integer> aggregatedColumnsIndexes = new LinkedHashMap<>();


    private final List<BiFunction<Object, Object, Object>> aggregationFunctions;
    private final List<Object> defaultValues; // 0 for count, null for the rest

    public AggregatedValues(ResultSet rs, List<DataColumn> columns) {
        this.rs = rs;
        this.columns = columns;

        int gi = 0;
        int ai = 0;
        for (DataColumn c : columns) {
            DataColumnRole role = c.getRole();
            nameToColumn.put(c.getName(), c);
            switch (role) {
                case AGGREGATED:
                    aggregatedColumnsIndexes.put(c.getName(), ai);
                    ai++;
                    break;
                case GROUP:
                    groupByColumnsIndexes.put(c.getName(), gi);
                    gi++;
                    break;
            }
        }

        aggregationFunctions = aggregatedColumnsIndexes.keySet().stream().map(nameToColumn::get)
                .map(DataColumn::getName)
                .map(functionPattern::matcher)
                .filter(Matcher::find) // TODO: throw exception if pattern is not matched
                .map(m -> m.group(1))
                .map(this::getFunction) // TODO: throw exception if function is not found
                .collect(Collectors.toList());

        defaultValues = aggregationFunctions.stream().map(f -> count == f ? 0 : null).collect(Collectors.toList());
    }


    static Object cast(Number result) {
        @SuppressWarnings("unchecked")
        Class<Object> minimalType = TypeTransformer.getMinimalType(result);
        return Double.class.equals(minimalType) ? result : TypeTransformer.safeCast(result, minimalType);
    }

    private BiFunction<Object, Object, Object> getFunction(String name) {
        BiFunction<Object, Object, Object> f = functions.get(name);
        if (f == null) {
            try {
                f = functionClasses.get(name).getConstructor().newInstance();
            } catch (ReflectiveOperationException e) {
                SneakyThrower.sneakyThrow(new SQLException(e));
            }
        }
        return f;
    }


    public List<List<?>> read() {
        while (next(rs)) {
            List<Object> group = groupByColumnsIndexes.keySet().stream().map(name -> getObject(rs, name)).collect(Collectors.toList());
            List<Object> aggregatedLine = aggregatedColumnsIndexes.keySet().stream().map(name -> getObject(rs, getDataColumnName(name))).collect(Collectors.toList());
            List<Object> aggregated = aggregate(aggregatedResults.computeIfAbsent(group, objects -> new ArrayList<>(defaultValues)), aggregatedLine);

            int i = 0;
            for (String name : groupByColumnsIndexes.keySet()) {
                updateType(nameToColumn.get(name), group.get(i));
                i++;
            }

            i = 0;
            for (String name : aggregatedColumnsIndexes.keySet()) {
                updateType(nameToColumn.get(name), aggregated.get(i));
                i++;
            }

            aggregatedResults.put(group, aggregated);
        }

        return aggregatedResults.entrySet().stream().map(ga -> {
            List<Object> row = new ArrayList<>();
            for (DataColumn c : columns) {
                DataColumnRole role = c.getRole();
                String name = c.getName();
                switch (role) {
                    case AGGREGATED:
                        row.add(ga.getValue().get(aggregatedColumnsIndexes.get(name)));
                        break;
                    case GROUP:
                        row.add(ga.getKey().get(groupByColumnsIndexes.get(name)));
                       break;
                    default: throw new IllegalStateException("");
                }
            }


            return row;
        }).collect(Collectors.toList());
    }


    private boolean next(ResultSet rs) {
        try {
            return rs.next();
        } catch (SQLException e) {
            SneakyThrower.sneakyThrow(e);
            return false;
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
            return "*".equals(label) ? 0 : rs.getObject(label);
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

    private static Double value(Object v) {
        return Optional.ofNullable(v).map(value -> TypeTransformer.cast(value, Double.class, 0.0)).orElse(0.0);
    }

    private static String getDataColumnName(String expr) {
        Matcher m = functionPattern.matcher(expr);
        if (m.find()) {
            return m.group(2);
        }
        throw new IllegalArgumentException(expr);
    }
}
