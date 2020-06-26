package com.nosqldriver.aerospike.sql.query;

import com.nosqldriver.aerospike.sql.AerospikeDatabaseMetadata;
import com.nosqldriver.util.SneakyThrower;

import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

public enum ShowRetriever implements BiFunction<AerospikeDatabaseMetadata, String, List<String>> {
    SCHEMAS("SCHEMA_NAME") {
        @Override
        public List<String> apply(AerospikeDatabaseMetadata md, String p) {
            return md.getCatalogNames();
        }
    },
    CATALOGS("SCHEMA_NAME") {
        @Override
        public List<String> apply(AerospikeDatabaseMetadata md, String p) {
            return md.getCatalogNames();
        }
    },
    TABLES("TABLE_NAME") {
        @Override
        public List<String> apply(AerospikeDatabaseMetadata md, String schema) {
            if (schema == null) {
                SneakyThrower.sneakyThrow(new SQLException("No namespace selected"));
            }


            return md.getTableNames(schema);
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
}
