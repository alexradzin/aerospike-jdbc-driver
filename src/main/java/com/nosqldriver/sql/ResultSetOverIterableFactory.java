package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.Map;

import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_INDEX;
import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_NAME;
import static com.nosqldriver.sql.ResultSetInvocationHandler.METADATA;
import static com.nosqldriver.sql.ResultSetInvocationHandler.NEXT;

public class ResultSetOverIterableFactory {
    private final ResultSetWrapperFactory wrapperFactory = new ResultSetWrapperFactory();

    public ResultSet create(String schema, String[] names, String[] aliases, Iterable<Object> iterable) {
        return wrapperFactory.create(new ResultSetInvocationHandler<Iterable<Object>>(NEXT | METADATA | GET_NAME | GET_INDEX, iterable, schema, names, aliases) {
            private final Iterator<Object> it = iterable.iterator();
            private Object current = null;
            private boolean currentInitialized = false;
            @Override
            protected boolean next() {
                if (it.hasNext()) {
                    current = it.next();
                    currentInitialized = true;
                    return true;
                }
                current = null;
                currentInitialized = false;
                return false;
            }

            @Override
            protected <T> T get(int i, Class<T> type) {
                int index = i - 1;
                String name = index < names.length ? names[index] : null;
                return get(name, type);
            }

            @Override
            protected <T> T get(String name, Class<T> type) {
                if(!currentInitialized) {
                    throw new IllegalStateException("Attempt to get value without next()");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> row = (Map<String, Object>)current;
                return cast(row.get(name), type);
            }
        });
    }
}
