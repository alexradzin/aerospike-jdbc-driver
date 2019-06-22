package com.nosqldriver.aerospike.sql.query;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.function.Function;
import java.util.function.Supplier;

public class JoinHolder {
    private final Function<ResultSet, ResultSet> resultSetRetriver;
    private final Supplier<ResultSetMetaData> metaDataSupplier;
    private final boolean skipIfMissing;


    public JoinHolder(Function<ResultSet, ResultSet> resultSetRetriver, Supplier<ResultSetMetaData> metaDataSupplier, boolean skipIfMissing) {
        this.resultSetRetriver = resultSetRetriver;
        this.metaDataSupplier = metaDataSupplier;
        this.skipIfMissing = skipIfMissing;
    }

    public Function<ResultSet, ResultSet> getResultSetRetriever() {
        return resultSetRetriver;
    }

    public Supplier<ResultSetMetaData> getMetaDataSupplier() {
        return metaDataSupplier;
    }

    public boolean isSkipIfMissing() {
        return skipIfMissing;
    }
}
