package com.nosqldriver.sql;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class SimpleParameterMetaData implements ParameterMetaData, SimpleWrapper {
    private final int count;

    public SimpleParameterMetaData(int count) {
        this.count = count;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return count;
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return false; //TODO: think what to do here in case of schema-less DB. It should be true for numbers and false otherwise.
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0; //TODO: think what to do here in case of schema-less DB. It should be true for numbers and false otherwise.
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0; //TODO: think what to do here in case of schema-less DB. It should be true for numbers and false otherwise.
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return Types.OTHER; //TODO: think what to do here in case of schema-less DB. It should be true for numbers and false otherwise.
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return "OTHER"; //TODO: think what to do here in case of schema-less DB. It should be true for numbers and false otherwise.
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return Object.class.getName(); //TODO: think what to do here in case of schema-less DB. It should be true for numbers and false otherwise.
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
