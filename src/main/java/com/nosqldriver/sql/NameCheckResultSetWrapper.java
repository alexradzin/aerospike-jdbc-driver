package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static com.nosqldriver.util.SneakyThrower.sneakyThrow;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class NameCheckResultSetWrapper extends ResultSetWrapper {
    public NameCheckResultSetWrapper(ResultSet rs, List<DataColumn> columns, boolean indexByName) {
        super(rs, columns, true);
    }

    @Override
    protected String getName(String alias) throws SQLException {
        return validate(alias);
    }


    private String validate(String alias) throws SQLException {
        List<DataColumn> columns = ((DataColumnBasedResultSetMetaData)getMetaData()).getColumns()
                .stream()
                .filter(c -> c.getRole() != HIDDEN || (c.getName() != null && c.getName().contains("["))).collect(toList());
        if (!columns.isEmpty() && columns.stream().noneMatch(c -> Objects.equals(alias, c.getLabel()) || (c.getLabel() == null && Objects.equals(alias, c.getName())))) {
            sneakyThrow(new SQLException(format("Column '%s' not found", alias)));
        }
        return alias;
    }
}
