package com.nosqldriver.util;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DataUtil {
    public static List<?> toList(Map<String, Object> map) {
        TreeMap<Integer, Object> m2 = map.entrySet().stream().collect(Collectors.toMap(e -> Integer.parseInt(e.getKey()), Map.Entry::getValue, (v1, v2) -> v2, TreeMap::new));
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

    public static Object[] toArray(Map<String, Object> map) {
        return toList(map).toArray();
    }
}
