package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.query.ResultSet;

public interface AerospikeQuery {
    //GET_BY_PK, STATEMENT,;
    ResultSet perform(IAerospikeClient client);
}
