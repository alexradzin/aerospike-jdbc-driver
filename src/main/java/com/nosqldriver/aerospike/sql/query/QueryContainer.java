package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;

import java.util.function.Function;

public interface QueryContainer<T> {
    Function<IAerospikeClient, T> getQuery();
    void setParameters(Object ... parameters);
}
