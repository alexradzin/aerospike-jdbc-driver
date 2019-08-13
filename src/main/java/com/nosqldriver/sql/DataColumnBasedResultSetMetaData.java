package com.nosqldriver.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static com.nosqldriver.sql.SqlLiterals.sqlTypeNames;

public class DataColumnBasedResultSetMetaData implements ResultSetMetaData {
    private final String schema;
    private final String table;
    private final List<DataColumn> columns;
    private boolean discovered;

    public DataColumnBasedResultSetMetaData(String schema, String table) {
        this(schema, table, Collections.emptyList());
    }

    public DataColumnBasedResultSetMetaData(List<DataColumn> columns) {
        this(null, null, columns);
    }

    private DataColumnBasedResultSetMetaData(String schema, String table, List<DataColumn> columns) {
        this.schema = schema;
        this.table = table;
        this.columns = Collections.unmodifiableList(columns);
    }

    public List<DataColumn> getColumns() {
        return columns;
    }

    public DataColumnBasedResultSetMetaData updateData(ResultSetMetaData md) throws SQLException {
        int n = md.getColumnCount();
        for (int i = 0; i < n; i++) {
            int index = i + 1;
            String catalog = md.getCatalogName(index);
            String table = md.getTableName(index);
            String label = md.getColumnLabel(index);
            int type = md.getColumnType(index);

            for (DataColumn column : columns) {
                if (Objects.equals(column.getName(), label)) {
                    if (column.getCatalog() == null) {
                        column.withCatalog(catalog);
                    }
                    if (column.getTable() == null) {
                        column.withTable(table);
                    }
                    if (Objects.equals(column.getCatalog(), catalog) && Objects.equals(column.getTable(), table) && column.getType() == 0) {
                        column.withType(type);
                    }
                }
            }

//            columns.stream()
//                    .filter(c -> Objects.equals(c.getCatalog(), catalog) && Objects.equals(c.getTable(), table) && Objects.equals(c.getName(), name))
//                    .findFirst().map(c -> c.withType(type));
        }
        return new DataColumnBasedResultSetMetaData(columns);
    }

    public void setDiscovered(boolean discovered) {
        this.discovered = discovered;
    }

    public boolean isDiscovered() {
        return discovered;
    }

    private Stream<DataColumn> getVisibleColumns() {
        return columns.stream().filter(c -> !HIDDEN.equals(c.getRole()));
    }

    private <T> T getVisibleColumn(int column, Function<DataColumn, T> getter) throws SQLException {
        if (column <= 0) {
            throw ExceptionFactory.invalidColumnIndex(column);
        }
        Optional<DataColumn> opt = getVisibleColumns().skip(column - 1).findFirst();
        if (!opt.isPresent()) {
            throw ExceptionFactory.invalidColumnIndex(column);
        }
        return getter.apply(opt.get());
    }

    @Override
    public int getColumnCount() {
        return (int)getVisibleColumns().count();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return true;
    }

    @Override
    public boolean isSearchable(int column) {
        return false; //TODO: if indexed
    }

    @Override
    public boolean isCurrency(int column) {
        return false;
    }

    @Override
    public int isNullable(int column) {
        return columnNullable; // any column in aerospike is nullable
    }

    @Override
    public boolean isSigned(int column) {
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getVisibleColumn(column, c -> Optional.ofNullable(c.getLabel()).orElseGet(c::getName));
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getVisibleColumn(column, c -> DataColumn.DataColumnRole.EXPRESSION.equals(c.getRole()) ? c.getExpression() : c.getName());
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        if (columns.isEmpty()) {
            throw ExceptionFactory.invalidColumnIndex(column);
        }
        return toEmpty(getVisibleColumn(column, DataColumn::getCatalog)); //TODO: ??? schema vs catalog
    }

    @Override
    public int getPrecision(int column) {
        return 0;
    }

    @Override
    public int getScale(int column) {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        if (columns.isEmpty()) {
            throw ExceptionFactory.invalidColumnIndex(column);
        }
        return toEmpty(getVisibleColumn(column, DataColumn::getTable));
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        if (columns.isEmpty()) {
            throw ExceptionFactory.invalidColumnIndex(column);
        }
        return toEmpty(getVisibleColumn(column, DataColumn::getCatalog));
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getVisibleColumn(column, DataColumn::getType);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return sqlTypeNames.get(getColumnType(column));
    }

    @Override
    public boolean isReadOnly(int column) {
        return false;
    }

    @Override
    public boolean isWritable(int column) {
        return true;
    }

    @Override
    public boolean isDefinitelyWritable(int column) {
        return true;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return SqlLiterals.sqlToJavaTypes.get(getColumnType(column)).getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    private String toEmpty(String s) {
        return s == null ? "" : s;
    }
}
