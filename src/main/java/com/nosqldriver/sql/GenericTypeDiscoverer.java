package com.nosqldriver.sql;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.SqlLiterals.getSqlType;

public class GenericTypeDiscoverer<R> implements TypeDiscoverer {
    private BiFunction<String, String, Iterable<R>> recordsFetcher;
    private Function<R, Map<String, Object>> toMap;
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

        List<DataColumn> result = all ? new ArrayList<>() : columns;
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
                    result.addAll(data.entrySet().stream()
                            .map(e -> DATA.create(catalog, table, e.getKey(), e.getKey()).withType(getSqlType(e.getValue())))
                            .collect(Collectors.toList()));
                } else {
                    ctd.getValue().forEach(c -> {
                        Object value = data.get(c.getName());
                        if (value != null) {
                            c.withType(getSqlType(value));
                        }
                        if (c.getLabel() == null) {
                            c.withLabel(c.getName());
                        }
                    });
                }
            }
        }

        return result;
    }
}
