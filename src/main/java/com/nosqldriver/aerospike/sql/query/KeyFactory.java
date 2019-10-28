package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.Key;
import com.nosqldriver.VisibleForPackage;

import static java.lang.String.format;

@VisibleForPackage
class KeyFactory {
    @VisibleForPackage
    static Key createKey(String schema, String table, Object value) {
        final Key key;
        if (value instanceof Byte) {
            key = new Key(schema, table, ((Byte) value).intValue());
        } else if (value instanceof Short) {
            key = new Key(schema, table, ((Short) value).intValue());
        } else if (value instanceof Integer) {
            key = new Key(schema, table, (Integer) value);
        } else if (value instanceof Long) {
            key = new Key(schema, table, (Long) value);
        } else if (value instanceof Number) {
            key = new Key(schema, table, ((Number) value).longValue());
        } else if (value instanceof String) {
            key = new Key(schema, table, (String) value);
        } else if (value instanceof byte[]) {
            key = new Key(schema, table, (byte[]) value);
        } else {
            throw new IllegalArgumentException(format("Filter by %s is not supported right now. Use either number or string", value == null ? null : value.getClass()));
        }
        return key;
    }
}
