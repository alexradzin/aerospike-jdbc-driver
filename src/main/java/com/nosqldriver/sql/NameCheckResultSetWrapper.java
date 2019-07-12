package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import static java.lang.String.format;

public class NameCheckResultSetWrapper extends ResultSetWrapper {
    public NameCheckResultSetWrapper(ResultSet rs, List<DataColumn> columns) {
        super(rs, columns);
    }

    @Override
    protected String getName(String alias) throws SQLException {
        return validate(alias);
    }


    private String validate(String alias) throws SQLException {
        List<DataColumn> columns = ((DataColumnBasedResultSetMetaData)getMetaData()).getColumns();
        if (!columns.isEmpty() && columns.stream().noneMatch(c -> Objects.equals(alias, c.getLabel()) || (c.getLabel() == null && Objects.equals(alias, c.getName())))) {
            throwAny(new SQLException(format("Column '%s' not found", alias)));
        }
        return alias;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwAny(Throwable e) throws E {
        throw (E)e;
    }
}
