package com.nosqldriver.sql;

import java.util.Objects;

public class DataColumn {
    private String catalog;
    private String table;
    private final String name;
    private String label;
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

    public DataColumn withLabel(String label) {
        this.label = label;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataColumn that = (DataColumn) o;
        return type == that.type &&
                Objects.equals(catalog, that.catalog) &&
                Objects.equals(table, that.table) &&
                Objects.equals(name, that.name) &&
                Objects.equals(label, that.label) &&
                Objects.equals(expression, that.expression) &&
                role == that.role;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, table, name, label, expression, type, role);
    }
}
