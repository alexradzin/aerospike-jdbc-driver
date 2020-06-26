package com.nosqldriver.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.lang.Integer.parseInt;
import static java.util.stream.Collectors.toMap;

public class DataUtil {
    public List<?> toList(Object arg) {
        if (arg instanceof List) {
            return (List<?>)arg;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>)arg;
        NavigableMap<Integer, Object> m2 = map.entrySet().stream().collect(toMap(e -> parseInt(e.getKey()), Entry::getValue, (v1, v2) -> v2, TreeMap::new));
        int n = m2.size();
        if (n != map.size()) {
            SneakyThrower.sneakyThrow(new SQLException("Cannot create list due to duplicate entries"));
        }
        if (n > 0) {
            int first = m2.firstKey();
            int last = m2.lastKey();
            if (first != 0 || last != n - 1) {
                SneakyThrower.sneakyThrow(new SQLException("Cannot create list due to missing entries"));
            }
        }
        return new ArrayList<>(m2.values());
    }

    public Object[] toArray(Object map) {
        return toList(map).toArray();
    }
}
