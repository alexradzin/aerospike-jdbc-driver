package com.nosqldriver.sql;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static com.nosqldriver.sql.SqlLiterals.getSqlType;
import static java.sql.Types.OTHER;
import static java.util.stream.Collectors.toList;

public class GenericTypeDiscoverer<R> implements TypeDiscoverer {
    private BiFunction<String, String, Iterable<R>> recordsFetcher;
    private Function<R, Map<String, Object>> toMap;
    private static final Predicate<Method> getter = method -> {
        String name = method.getName();
        return (name.startsWith("get") || name.startsWith("is")) && !void.class.equals(method.getReturnType()) && method.getParameterCount() == 0;
    };
    private static final Function<Method, String> propertyNameRetriever = method -> {
        String name = method.getName().replaceFirst("^get|is", "");
        return name.substring(0, 1).toLowerCase() + name.substring(1);
    };
    private final int limit;


    public GenericTypeDiscoverer(BiFunction<String, String, Iterable<R>> recordsFetcher, Function<R, Map<String, Object>> toMap) {
        this(recordsFetcher, toMap, 1);
    }

    public GenericTypeDiscoverer(BiFunction<String, String, Iterable<R>> recordsFetcher, Function<R, Map<String, Object>> toMap, int limit) {
        this.recordsFetcher = recordsFetcher;
        this.toMap = toMap;
        this.limit = limit;
    }

    @Override
    public List<DataColumn> discoverType(List<DataColumn> columns) {
        boolean all = columns.size() == 1 && "*".equals(columns.get(0).getName());

        List<DataColumn> mainColumns = all ? new ArrayList<>() : columns;
        List<DataColumn> subColumns = new ArrayList<>();
        Map<String, List<DataColumn>> columnsByTable = columns.stream().collect(Collectors.groupingBy(c -> c.getCatalog() + "." + c.getTable()));
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
                            .map(e -> DATA.create(catalog, table, e.getKey(), e.getKey()).withType(getSqlType(e.getValue())))
                            .collect(toList()));
                } else {
                    ctd.getValue().forEach(c -> {
                        Object value = data.get(c.getName());
                        if (value != null) {
                            int sqlType = getSqlType(value);
                            c.withType(sqlType);
                            if (sqlType == OTHER) {
                                subColumns.addAll(extractFieldTypes(c, value.getClass()));
                            }

                        }
                        if (c.getLabel() == null) {
                            c.withLabel(c.getName());
                        }
                    });
                }
            }
        }

        List<DataColumn> result = subColumns.isEmpty() ? mainColumns : Stream.concat(mainColumns.stream(), subColumns.stream()).collect(Collectors.toList());
        return result;
    }


    private Collection<DataColumn> extractFieldTypes(DataColumn column, Class<?> clazz) {
        return Arrays.stream(clazz.getMethods())
                .filter(getter)
                .map(g -> {
                    String name = String.format("%s[%s]", column.getName(), propertyNameRetriever.apply(g));
                    int type = SqlLiterals.sqlTypes.getOrDefault(g.getReturnType(), OTHER);
                    return HIDDEN.create(column.getCatalog(), column.getTable(), name, name).withType(type);
                })
                .collect(Collectors.toList());
    }

}
