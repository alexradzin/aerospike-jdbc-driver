package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_INDEX;
import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_NAME;
import static com.nosqldriver.sql.ResultSetInvocationHandler.METADATA;
import static com.nosqldriver.sql.ResultSetInvocationHandler.NEXT;
import static com.nosqldriver.sql.TypeTransformer.cast;

public class ResultSetOverDistinctMapFactory {
    private final ResultSetWrapperFactory wrapperFactory = new ResultSetWrapperFactory();

    public ResultSet create(String schema, String[] names, String[] aliases, Iterable<Object> iterable) {
        return wrapperFactory.create(new ResultSetInvocationHandler<Iterable<Object>>(NEXT | METADATA | GET_NAME | GET_INDEX, iterable, schema, names, aliases) {
            private final Iterator<Object> it = iterable.iterator();
            private Map<String, Object> row = null;
            private List<Entry<String, Object>> entries = null;
            private int currentIndex = -1;

            @Override
            protected boolean next() {
                if (currentIndex < 0 && !it.hasNext()) {
                    return false;
                }
                currentIndex++;
                if (currentIndex == 0) {
                    row = toMap(it.next());
                    entries = new ArrayList<>(row.entrySet());
                    currentIndex = 0;
                    return true;
                }
                return currentIndex < row.size();
            }

            @Override
            protected <T> T get(int i, Class<T> type) {
                int index = i - 1;
                Entry<String, Object> e = entries.get(currentIndex);
                return cast(index == 0 ? e.getKey() : toMap(e.getValue()).get(names[index]), type);
            }

            @Override
            protected <T> T get(String name, Class<T> type) {
                return cast(row.get(name), type);
            }
        });
    }


    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> toMap(Object obj) {
        return (Map<K, V>)obj;
    }
}
