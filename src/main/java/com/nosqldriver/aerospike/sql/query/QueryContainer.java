package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;

import java.sql.Statement;
import java.util.function.Function;

public interface QueryContainer<T> {
    Function<IAerospikeClient, T> getQuery(Statement sqlStatement);
    void setParameters(Statement sqlStatement, Object ... parameters);
}
