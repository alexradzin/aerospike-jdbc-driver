package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.AerospikeResultSetMetaData;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

import static java.lang.String.format;

public class ResultSetOverIterableFactory {
    public ResultSet create(String schema, String[] names, String[] aliases, Iterable<Object> iterable) {
        return (ResultSet) Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class[]{ResultSet.class}, new InvocationHandler() {
            private final Iterator<Object> it = iterable.iterator();
            private Object current = null;
            private boolean currentInitialized = false;
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                if ("next".equals(method.getName())) {
                    if (it.hasNext()) {
                        current = it.next();
                        currentInitialized = true;
                        return true;
                    }
                    current = null;
                    currentInitialized = false;
                    return false;
                }
                if ("getMetaData".equals(method.getName())) {
                    return new AerospikeResultSetMetaData(null, schema, names, aliases);
                }

                if(!currentInitialized) {
                    throw new SQLException(format("Attempt to call %s without next()", method.getName()));
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> row = (Map<String, Object>)current;
                if (method.getName().startsWith("get") && method.getParameterTypes().length == 1) {
                    final String name;
                    if (int.class == method.getParameterTypes()[0]) {
                        int index = (int)args[0] - 1;
                        name = index < names.length ? names[index] : null;
                    } else if (String.class == method.getParameterTypes()[0]) {
                        name = (String)args[0];
                    } else {
                        throw new IllegalArgumentException(method.toString());
                    }
                    return cast(row.get(name), method.getReturnType());
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

        return (T)obj;
    }
}
