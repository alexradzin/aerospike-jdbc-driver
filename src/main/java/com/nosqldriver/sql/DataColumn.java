package com.nosqldriver.sql;

public class DataColumn {
    private String catalog;
    private String table;
    private final String name;
    private final String label;
    private final String expression;
    private int type;

    private final DataColumnRole role;

    public enum DataColumnRole {
        DATA, HIDDEN, AGGREGATED, GROUP, EXPRESSION {
            @Override
            public DataColumn create(String catalog, String table, String expression, String label) {
                return new DataColumn(catalog, table, null, label, expression, this);
            }
        },
        ;

        public DataColumn create(String catalog, String table, String name, String label) {
            return new DataColumn(catalog, table, name, label, null, this);
        }
    }


    private DataColumn(String catalog, String table, String name, String label, String expression, DataColumnRole role) {
        this.catalog = catalog;
        this.table = table;
        this.name = name;
        this.label = label;
        this.expression = expression;
        this.role = role;
    }


    public DataColumn withCatalog(String catalog) {
        this.catalog = catalog;
        return this;
    }

    public DataColumn withTable(String table) {
        this.table = table;
        return this;
    }

    public DataColumn withType(int type) {
        this.type = type;
        return this;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getTable() {
        return table;
    }

    public String getName() {
        return name;
    }

    public String getLabel() {
        return label;
    }

    public String getExpression() {
        return expression;
    }

    public int getType() {
        return type;
    }

    public DataColumnRole getRole() {
        return role;
    }
}
