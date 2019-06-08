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
    // Important: corresponding constant is defined in groupby.lua
    private static final String KEY_DELIMITER = "_nsqld_as_d_";
    private final ResultSetWrapperFactory wrapperFactory = new ResultSetWrapperFactory();

    public ResultSet create(String schema, String[] names, String[] aliases, Iterable<Object> iterable) {
        return wrapperFactory.create(new ResultSetInvocationHandler<Iterable<Object>>(NEXT | METADATA | GET_NAME | GET_INDEX, iterable, schema, names, aliases) {
            private final Iterator<Object> it = iterable.iterator();
            private Map<Object, Object> row = null;
            private List<Entry<Object, Object>> entries = null;
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
                if(currentIndex < 0) {
                    return null; //TODO: is this correct? Should exception be thrown here?
                }
                int index = i - 1;
                Entry<Object, Object> e = entries.get(currentIndex);
                Object key = e.getKey();
                Object[] keys = key instanceof String ? ((String)key).split(KEY_DELIMITER) : new Object[] {key};
                return cast(index < keys.length ? keys[index] : toMap(e.getValue()).get(names[index]), type);
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
