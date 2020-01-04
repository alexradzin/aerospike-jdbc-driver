package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.nosqldriver.aerospike.sql.query.AerospikeInsertQuery;
import com.nosqldriver.sql.ListRecordSet;
import com.nosqldriver.sql.SimpleWrapper;
import com.nosqldriver.util.ThrowingSupplier;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;

public class AerospikeStatement implements java.sql.Statement, SimpleWrapper {
    protected final IAerospikeClient client;
    private final Connection connection;
    protected final AtomicReference<String> schema;
    protected String set;
    private int maxRows = Integer.MAX_VALUE;
    private int queryTimeout = 0;
    private volatile SQLWarning sqlWarning;
    protected final AerospikePolicyProvider policyProvider;
    protected final Collection<String> indexes;
    private ResultSet resultSet;
    private int updateCount;

    protected enum StatementType implements Predicate<String> {
        SELECT {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                AerospikeQueryFactory aqf = statement.createQueryFactory();
                Function<IAerospikeClient, ResultSet> query = aqf.createQueryPlan(sql).getQuery(statement);
                statement.set = aqf.getSet();
                return query.apply(statement.client);
            }

            @Override
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                statement.resultSet = executeQuery(statement, sql);
                return true;
            }
        },
        INSERT {
            int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
                AerospikeQueryFactory aqf = statement.createQueryFactory();
                Function<IAerospikeClient, ResultSet> insert = aqf.createQueryPlan(sql).getQuery(statement);
                insert.apply(statement.client);
                statement.set = aqf.getSet();
                statement.setUpdateCount(ofNullable(AerospikeInsertQuery.updatedRecordsCount.get()).orElse(0));

                return statement.getUpdateCount();
            }
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                return executeUpdate(statement, sql) > 0;
            }
        },
        UPDATE {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                executeUpdate(statement, sql);
                return new ListRecordSet(statement, statement.schema.get(), statement.set, emptyList(), emptyList());
            }
            @Override
            int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
                AerospikeQueryFactory aqf = statement.createQueryFactory();
                Function<IAerospikeClient, Integer> update = aqf.createUpdate(statement, sql);
                statement.set = aqf.getSet();
                int count = update.apply(statement.client);
                statement.setUpdateCount(count);
                return count;
            }

            @Override
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                return executeUpdate(statement, sql) > 0;
            }
        },
        DELETE(UPDATE),
        SHOW,
        USE {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                String schema = execute(statement, sql) ? statement.schema.get() : null;
                return new ListRecordSet(statement, schema, null, emptyList(), emptyList());
            }

            @Override
            int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
                return execute(statement, sql) ? 1 :  0;
            }

            @Override
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                AerospikeQueryFactory aqf = statement.createQueryFactory();
                aqf.createQueryPlan(sql);
                statement.schema.set(aqf.getSchema());
                return statement.schema.get() != null;
            }
        },

        CREATE_INDEX {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                executeUpdate(statement, sql);
                return new ListRecordSet(statement, statement.schema.get(), statement.set, emptyList(), emptyList());
            }
            @Override
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                return executeUpdate(statement, sql) > 0;
            }
            @Override
            int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
                Matcher m = fixIndexPattern.matcher(sql);
                if (m.find()) {
                    sql = sql.substring(0, m.start()) + sql.substring(m.start(), m.end()).replaceAll("\\s+", ".") + sql.substring(m.end());
                }

                List<String> indexes = new ArrayList<>();
                AerospikeQueryFactory aqf = new AerospikeQueryFactory(statement, statement.schema.get(), statement.policyProvider, indexes);
                aqf.createQueryPlan(sql);
                String[] index = aqf.getIndexes().iterator().next().split("\\.");

                IndexType indexType = IndexType.valueOf(index[0].toUpperCase());
                String binName = index[3];
                String indexName = index[4];
                IndexCollectionType indexCollectionType = IndexCollectionType.DEFAULT;
                if (index.length == 6) {
                    binName = index[4];
                    indexName = index[5];
                    indexCollectionType = IndexCollectionType.valueOf(index[1].toUpperCase());
                }

                IndexTask task = statement.client.createIndex(null, aqf.getSchema(), aqf.getSet(), indexName, binName, indexType, indexCollectionType);
                int timeout = statement.client.getWritePolicyDefault().totalTimeout;
                if(timeout > 0) {
                    task.waitTillComplete(timeout);
                } else {
                    task.waitTillComplete();
                }
                return 1;
            }

            @Override
            public boolean test(String sql) {
                return createIndexPattern.matcher(sql).find();
            }
        },

        DROP_INDEX {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                executeUpdate(statement, sql);
                return new ListRecordSet(statement, statement.schema.get(), statement.set, emptyList(), emptyList());
            }
            @Override
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                return executeUpdate(statement, sql) > 0;
            }
            @Override
            int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
                List<String> indexes = new ArrayList<>();
                AerospikeQueryFactory aqf = new AerospikeQueryFactory(statement, statement.schema.get(), statement.policyProvider, indexes);
                aqf.createQueryPlan(sql);
                String indexName = aqf.getIndexes().iterator().next().split("\\.")[2];
                statement.client.dropIndex(null, aqf.getSchema(), aqf.getSet(), indexName);
                return 1;
            }
        },
        ;

        private final StatementType prototype;

        StatementType() {
            this(null);
        }
        StatementType(StatementType prototype) {
            this.prototype = prototype;
        }

        ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
            return executeAny(() -> prototype.executeQuery(statement, sql));
        }
        int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
            return executeAny(() -> prototype.executeUpdate(statement, sql));
        }
        boolean execute(AerospikeStatement statement, String sql) throws SQLException {
            return executeAny(() -> prototype.execute(statement, sql));
        }


        <R> R executeAny(ThrowingSupplier<R, SQLException> executor) throws SQLException {
            if (prototype != null) {
                return executor.get();
            }
            String methodName = new Throwable().getStackTrace()[1].getMethodName();
            throw new UnsupportedOperationException(format("%s does not support %s", name(), methodName));
        }



        @Override
        public boolean test(String sql) {
            return sql.toUpperCase().startsWith(name().replace("_", " ") + " ");
        }

    }

    private static final String indexTypes = "STRING|NUMERIC|GEO2DSPHERE";
    private static final String indexCollectionType = "DEFAULT|LIST|MAPKEYS|MAPVALUES";
    private static final Pattern createIndexPattern = Pattern.compile(format("^CREATE\\s+(%s)(\\s+(%s))?\\s+INDEX.*", indexTypes, indexCollectionType));
    private static final Pattern fixIndexPattern = Pattern.compile(format("((?:%s)\\s+(?:%s))", indexTypes, indexCollectionType));



    public AerospikeStatement(IAerospikeClient client, Connection connection, AtomicReference<String> schema, AerospikePolicyProvider policyProvider) {
        this.client = client;
        this.connection = connection;
        this.schema = schema;
        this.policyProvider = policyProvider;
        indexes = new ConnectionParametersParser().indexesParser(Info.request(client.getNodes()[0], "sindex"));
    }



    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        try {
            return getStatementType(sql).executeQuery(this, sql);
        } catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            return getStatementType(sql).executeUpdate(this, sql);
        } catch (RuntimeException e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        // nothing to do here
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 8 * 1024 * 1024; //8 MB - the Aerospike limitation
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new SQLFeatureNotSupportedException("Max field size cannot be changed dynamically");
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // nothing to do here so far
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Statement cannot be canceled");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return sqlWarning;
    }

    @Override
    public void clearWarnings() throws SQLException {
        sqlWarning = null;
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursor is not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return getStatementType(sql).execute(this, sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return updateCount;
    }

    public void setUpdateCount(int updateCount) {
        this.updateCount = updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLException(format("Attempt to set unsupported fetch direction %d. Only FETCH_FORWARD=%d is supported. The value is ignored.", direction, FETCH_FORWARD));
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows != 1) {
            throw new SQLException("Fetch size other than 1 is not supported right now");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch update is not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch update is not supported");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch update is not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return new ListRecordSet(this, null, null, emptyList(), emptyList());
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (poolable) {
            throw new SQLFeatureNotSupportedException("Statement does not support pools");
        }
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // just ignore
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    protected static StatementType getStatementType(String sql) throws SQLException {
        String sqlUp = sql.trim().toUpperCase();
        Optional<StatementType> type = Arrays.stream(StatementType.values()).filter(t -> t.test(sqlUp)).findFirst();

        if (!type.isPresent()) {
            throw new SQLSyntaxErrorException(format("SQL statement %s is not supported. SQL should start with one of: %s", sql, Arrays.toString(Arrays.stream(StatementType.values()).map(Enum::name).map(name -> name.replace("_", " ")).toArray())));
        }
        return type.get();
    }


    protected AerospikeQueryFactory createQueryFactory() {
        return new AerospikeQueryFactory(this, schema.get(), policyProvider, indexes);
    }
}
