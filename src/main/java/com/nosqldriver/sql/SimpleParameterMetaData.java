package com.nosqldriver.sql;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Optional;

import static com.nosqldriver.sql.SqlLiterals.sqlTypeNames;

public class SimpleParameterMetaData implements ParameterMetaData, SimpleWrapper {
    private final List<DataColumn> columns;

    public SimpleParameterMetaData(List<DataColumn> columns) {
        this.columns = columns;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return columns.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return DataColumnBasedResultSetMetaData.precisionByType.getOrDefault(getParameterType(param), 0);
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return columns.get(param - 1).getType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return sqlTypeNames.get(getParameterType(param));
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return Optional.ofNullable(SqlLiterals.sqlToJavaTypes.get(getParameterType(param))).map(Class::getName).orElse(null);
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
