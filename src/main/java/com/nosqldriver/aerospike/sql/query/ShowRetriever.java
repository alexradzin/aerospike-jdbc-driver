package com.nosqldriver.aerospike.sql.query;

import com.nosqldriver.aerospike.sql.AerospikeDatabaseMetadata;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.util.SneakyThrower;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public enum ShowRetriever implements BiFunction<AerospikeDatabaseMetadata, String, List<List<?>>> {
    SCHEMAS("SCHEMA_NAME") {
        @Override
        public List<List<?>> apply(AerospikeDatabaseMetadata md, String p) {
            return transform(md.getCatalogNames());
        }
    },
    CATALOGS("SCHEMA_NAME") {
        @Override
        public List<List<?>> apply(AerospikeDatabaseMetadata md, String p) {
            return transform(md.getCatalogNames());
        }
    },
    TABLES("TABLE_NAME") {
        @Override
        public List<List<?>> apply(AerospikeDatabaseMetadata md, String schema) {
            if (schema == null) {
                SneakyThrower.sneakyThrow(new SQLException("No namespace selected"));
            }
            return transform(md.getTableNames(schema));
        }
    },
    INDEXES("INDEXES") {
        @Override
        public List<List<?>> apply(AerospikeDatabaseMetadata md, String p) {
            return md.getIndexInfo();
        }
        public List<DataColumn> columns() {
            return Arrays.asList(column("TABLE_NAME"), column("INDEX_NAME"), column("COLUMN_NAME"), column("INDEX_TYPE"));
        }
        private DataColumn column(String name) {
            return DATA.create(null, null, name, name);
        }
    },
    ;

    private final String columnName;

    ShowRetriever(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public List<DataColumn> columns() {
        String column = getColumnName();
        return singletonList(DATA.create(null, null, column, column));
    }

    protected List<List<?>> transform(List<String> items) {
        return items.stream().map(Collections::singletonList).collect(toList());
    }
}
