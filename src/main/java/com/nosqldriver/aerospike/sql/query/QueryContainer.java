package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.nosqldriver.sql.DataColumn;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;

public interface QueryContainer<T> {
    Function<IAerospikeClient, T> getQuery(Statement sqlStatement) throws SQLException;
    void setParameters(Statement sqlStatement, Object ... parameters);
    List<DataColumn> getRequestedColumns();
    List<DataColumn> getFilteredColumns();
    String getSetName();
}
