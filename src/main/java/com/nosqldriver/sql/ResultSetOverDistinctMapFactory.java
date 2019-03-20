package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.AerospikeResultSetMetaData;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ResultSetOverDistinctMapFactory {
    public ResultSet create(String schema, String[] names, String[] aliases, Iterable<Object> iterable) {
        return (ResultSet) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{ResultSet.class}, new InvocationHandler() {
            private final Iterator<Object> it = iterable.iterator();
            private Map<String, Object> row = null;
            private List<Entry<String, Object>> entries = null;
            private int currentIndex = -1;
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if ("next".equals(method.getName())) {
                    if (currentIndex < 0 && !it.hasNext()) {
                        return false;
                    }
                    currentIndex++;
                    if (currentIndex == 0) {
                        row = (Map<String, Object>)it.next();
                        entries = row.entrySet().stream().collect(Collectors.toList());
                        currentIndex = 0;
                        return true;
                    }
                    return currentIndex < row.size();
                }
                if ("getMetaData".equals(method.getName())) {
                    return new AerospikeResultSetMetaData(null, schema, names, aliases);
                }

                if (method.getName().startsWith("get") && method.getParameterTypes().length == 1) {
                    final String name;
                    if (int.class == method.getParameterTypes()[0]) {
                        int index = (int)args[0] - 1;
                        Entry<String, Object> e = entries.get(currentIndex);
                        return cast(index == 0 ? e.getKey() : ((List<Object>)e.getValue()).get(index - 1), method.getReturnType());

                        //return cast(entries.get(index).getValue(), method.getReturnType());
                    } else if (String.class == method.getParameterTypes()[0]) {
                        name = (String)args[0];
                        return cast(row.get(name), method.getReturnType());
                    } else {
                        throw new IllegalArgumentException(method.toString());
                    }
                }

                throw new UnsupportedOperationException(method.getName());
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <T> T cast(Object obj, Class<T> type) {
        //TODO: create better implementation: probably base InvocationHandler for ResultSets
        if (ExpressionAwareResultSetFactory.typeTransforemers.containsKey(type) && obj instanceof Number) {
            return (T) ExpressionAwareResultSetFactory.typeTransforemers.get(type).apply((Number)obj);
        }

        if (ExpressionAwareResultSetFactory.typeTransforemers.containsKey(type) && obj instanceof String) {
            return (T)Integer.valueOf((String)obj); //TODO: patch!!!
        }
        return (T)obj;
    }
}
