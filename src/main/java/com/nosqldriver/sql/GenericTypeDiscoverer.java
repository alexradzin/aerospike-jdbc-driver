package com.nosqldriver.sql;

import com.nosqldriver.util.FunctionManager;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.PK;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.role;
import static com.nosqldriver.sql.SqlLiterals.getSqlType;
import static java.lang.String.format;
import static java.sql.Types.OTHER;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public class GenericTypeDiscoverer<R> implements TypeDiscoverer {
    private BiFunction<String, String, Iterable<R>> recordsFetcher;
    private Function<R, Map<String, Object>> toMap;
    private final FunctionManager functionManager;

    private static final Predicate<Method> getter = method -> {
        String name = method.getName();
        return (name.startsWith("get") || name.startsWith("is")) && !void.class.equals(method.getReturnType()) && method.getParameterCount() == 0;
    };
    private static final Function<Method, String> propertyNameRetriever = method -> {
        String name = method.getName().replaceFirst("^get|is", "");
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    };
    private final int limit;
    private final boolean pk;


    public GenericTypeDiscoverer(BiFunction<String, String, Iterable<R>> recordsFetcher, Function<R, Map<String, Object>> toMap, FunctionManager functionManager, boolean pk) {
        this(recordsFetcher, toMap, functionManager, 1, pk);
    }

    public GenericTypeDiscoverer(BiFunction<String, String, Iterable<R>> recordsFetcher, Function<R, Map<String, Object>> toMap, FunctionManager functionManager, int limit, boolean pk) {
        this.recordsFetcher = recordsFetcher;
        this.toMap = toMap;
        this.functionManager = functionManager;
        this.limit = limit;
        this.pk = pk;
    }

    @Override
    public List<DataColumn> discoverType(List<DataColumn> columns) {
        boolean all = columns.size() == 1 && "*".equals(columns.get(0).getName());

        List<DataColumn> mainColumns = all ? new ArrayList<>() : columns;
        List<DataColumn> subColumns = new ArrayList<>();
        Map<String, List<DataColumn>> dataColumnsByTable = columns.stream().collect(Collectors.groupingBy(c -> c.getCatalog() + "." + c.getTable()));
        Map<String, List<DataColumn>> columnsByTable = new HashMap<>(dataColumnsByTable);
        if (pk && all) {
            int nColumns = dataColumnsByTable.size();
            Map<String, List<DataColumn>> pkColumnsByTable = dataColumnsByTable.keySet().stream().map(catalogAndTable -> catalogAndTable.split("\\."))
                    .map(catalogAndTable -> PK.create(catalogAndTable[0], catalogAndTable[1], "PK", nColumns == 1 ? "PK" : catalogAndTable[1] + ".PK"))
                    .collect(Collectors.groupingBy(c -> c.getCatalog() + "." + c.getTable()));
            columnsByTable.putAll(pkColumnsByTable);

        }
        for (Map.Entry<String, List<DataColumn>> ctd : columnsByTable.entrySet()) {
            String[] ct = ctd.getKey().split("\\.");
            String catalog = ct[0];
            String table = ct[1];
            Iterator<R> it =  recordsFetcher.apply(catalog, table).iterator();

            for (int i = 0; it.hasNext() && i < limit; i++) {
                R r = it.next();
                Map<String, Object> data = toMap.apply(r);
                if (all) {
                    mainColumns.addAll(data.entrySet().stream()
                            .map(e -> role(e.getKey()).create(catalog, table, e.getKey(), e.getKey()).withType(getSqlType(e.getValue())))
                            .collect(toList()));
                } else {
                    ctd.getValue().forEach(c -> {
                        Object value = data.containsKey(c.getName()) ? data.get(c.getName()) : data.get(c.getLabel());
                        if (value == null && PK.equals(c.getRole()) && c.getName().startsWith(c.getTable())) {
                            value = data.get("PK");
                        }
                        if (value != null) {
                            int sqlType = getSqlType(value);
                            c.withType(sqlType);
                            if (value instanceof Map) {
                                Collection<DataColumn> mapColumns = extractFieldTypes(c, ((Map<String, Object>)value));
                                subColumns.addAll(mapColumns);
                            } else if (sqlType == OTHER) {
                                subColumns.addAll(extractFieldTypes(c, value.getClass()));
                            }
                        } else if (EXPRESSION.equals(c.getRole())) {
                            String functionName = c.getExpression().replaceFirst("\\(.*", "");
                            functionManager.getFunctionReturnType(functionName).map(clazz -> subColumns.addAll(extractFieldTypes(c, clazz)));
                        }
                        if (c.getLabel() == null) {
                            c.withLabel(c.getName());
                        }
                    });
                }
            }
        }

        return subColumns.isEmpty() ? mainColumns : concat(mainColumns.stream(), subColumns.stream()).collect(toList());
    }

    private Collection<DataColumn> extractFieldTypes(DataColumn column, Class<?> clazz) {
        Collection<DataColumn> allColumns = new ArrayList<>();
        return extractFieldTypes(allColumns, column, clazz);
    }


    private Collection<DataColumn> extractFieldTypes(Collection<DataColumn> allColumns, DataColumn column, Class<?> clazz) {
        Arrays.stream(clazz.getMethods())
                .filter(getter)
                .forEach(g -> {
                    DataColumn.DataColumnRole role = column.getRole();
                    String columnName = DATA.equals(role) ? column.getName() : EXPRESSION.equals(role) ? column.getExpression() : column.getLabel();
                    String propName = propertyNameRetriever.apply(g);
                    String name = columnName.charAt(columnName.length() - 1) == ']' ? columnName.substring(0, columnName.length() - 1) + "." + propName + "]" : format("%s[%s]", columnName, propName);
                    Class subType = g.getReturnType();
                    int type = SqlLiterals.sqlTypes.getOrDefault(subType, OTHER);
                    DataColumn subColumn = HIDDEN.create(column.getCatalog(), column.getTable(), name, name).withType(type);
                    allColumns.add(subColumn);
                    if (type == OTHER && !Object.class.equals(g.getDeclaringClass())) {
                       extractFieldTypes(allColumns, subColumn, subType);
                    }
                });
        return allColumns;
    }

    private Collection<DataColumn> extractFieldTypes(DataColumn column, Map<String, Object> map) {
        Collection<DataColumn> allColumns = new ArrayList<>();
        return extractFieldTypes(allColumns, column, map);
    }

    private Collection<DataColumn> extractFieldTypes(Collection<DataColumn> allColumns, DataColumn column, Map<String, Object> map) {
        for(Map.Entry<String, Object> e : map.entrySet()) {
            Object value = e.getValue();
            int type = SqlLiterals.sqlTypes.getOrDefault(Optional.ofNullable(value).map(Object::getClass).orElse(null), OTHER);
            String propName = e.getKey();
            String columnName = DATA.equals(column.getRole()) ? column.getName() : column.getLabel();
            String name = columnName.charAt(columnName.length() - 1) == ']' ? columnName.substring(0, columnName.length() - 1) + "." + propName + "]" : format("%s[%s]", columnName, propName);
            DataColumn subColumn = HIDDEN.create(column.getCatalog(), column.getTable(), name, name).withType(type);
            allColumns.add(subColumn);
            if (value instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> mapValue = (Map<String, Object>)value;
                extractFieldTypes(allColumns, subColumn, mapValue);
            }
        }
        return allColumns;
    }
}
