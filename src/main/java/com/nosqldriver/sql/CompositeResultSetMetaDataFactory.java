package com.nosqldriver.sql;

import com.nosqldriver.VisibleForPackage;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;

@VisibleForPackage
class CompositeResultSetMetaDataFactory {
    @VisibleForPackage
    ResultSetMetaData compose(Collection<ResultSetMetaData> metadataElements) throws SQLException {
        String[] names = null;
        String[] aliases = null;
        int[] types = null;
        String schema = null;

        boolean mainMetaData = true;
        int start = 0;
        for (ResultSetMetaData md : metadataElements) {
            int n = md.getColumnCount();
            if (mainMetaData) {
                names = new String[n];
                aliases = new String[n];
                types = new int[n];
                schema = md.getSchemaName(1);
            }

            for (int i = 0; i < n; i++) {
                int j = i + 1;
                String name = md.getColumnName(j);
                String alias = md.getColumnLabel(j);
                int type = md.getColumnType(j);
                schema = md.getSchemaName(j);
                if (mainMetaData) {
                    names[i] = name;
                    aliases[i] = alias;
                    types[i] = type;
                } else {
                    start = 1 + update(names, aliases, types, start, name, alias, type);
                }
            }
            mainMetaData = false;
        }

        return new SimpleResultSetMetaData(null, schema, names, aliases, types);
    }

    private int update(String[] names, String[] aliases, int[] types, int start, String name, String alias, int type) {
        int i = start;
        for (; i < names.length; i++) {
            if (name.equals(names[i])) {
                break;
            }
        }

        if (i >= names.length) {
            throw new IllegalStateException(String.format("Name %s is not found", name));
        }

        if (aliases[i] == null && alias != null) {
            aliases[i] = alias;
        }
        if (types[i] == 0 && type != 0) {
            types[i] = type;
        }

        return i;
    }
}
