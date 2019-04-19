package com.nosqldriver.aerospike.sql.query;

import java.sql.ResultSet;
import java.util.function.Function;

public class JoinHolder {
    private final Function<ResultSet, ResultSet> resultSetRetriver;
    private final boolean skipIfMissing;


    public JoinHolder(Function<ResultSet, ResultSet> resultSetRetriver, boolean skipIfMissing) {
        this.resultSetRetriver = resultSetRetriver;
        this.skipIfMissing = skipIfMissing;
    }

    public Function<ResultSet, ResultSet> getResultSetRetriver() {
        return resultSetRetriver;
    }

    public boolean isSkipIfMissing() {
        return skipIfMissing;
    }
}
