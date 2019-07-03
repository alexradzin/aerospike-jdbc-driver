package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;

public class NameCheckResultSetWrapper extends ResultSetWrapper {
    public NameCheckResultSetWrapper(ResultSet rs, List<String> names, List<String> aliases, List<DataColumn> columns) {
        super(rs, names, aliases, columns);
    }

    @Override
    protected String getName(String alias) {
        return validate(alias);
    }


    private String validate(String alias) {
        if (!aliases.contains(alias) && !names.contains(alias) && !names.isEmpty()) {
            throwAny(new SQLException(format("Column '%s' not found", alias)));
        }
        return alias;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwAny(Throwable e) throws E {
        throw (E)e;
    }

}
