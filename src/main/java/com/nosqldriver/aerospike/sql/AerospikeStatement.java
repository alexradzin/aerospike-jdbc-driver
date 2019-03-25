package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.nosqldriver.sql.ResultSetInvocationHandler;
import com.nosqldriver.sql.ResultSetWrapperFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Collection;

import static com.nosqldriver.sql.ResultSetInvocationHandler.METADATA;
import static com.nosqldriver.sql.ResultSetInvocationHandler.NEXT;

public class AerospikeStatement implements java.sql.Statement {
    private final IAerospikeClient client;
    private final Connection connection;
    private final String schema;
    private int maxRows = Integer.MAX_VALUE;
    private int queryTimeout = 0;
    private volatile SQLWarning sqlWarning;
    private final AerospikePolicyProvider policyProvider;
    private final Collection<String> indexes;
    private final ConnectionParametersParser parametersParser = new ConnectionParametersParser();

    private enum StatementType {
        SELECT {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                return new AerospikeQueryFactory(statement.schema, statement.policyProvider, statement.indexes).createQuery(sql).apply(statement.client);
            }
        },
        INSERT,
        UPDATE,
        DELETE {
            @Override
            ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
                executeUpdate(statement, sql);

                return new ResultSetWrapperFactory().create(new ResultSetInvocationHandler<Object>(NEXT | METADATA, null, statement.schema, new String[0], new String[0]) {
                    @Override
                    protected boolean next() {
                        return false;
                    }
                });
            }
            @Override
            int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
                return new AerospikeQueryFactory(statement.schema, statement.policyProvider, statement.indexes).createUpdate(sql).apply(statement.client);
            }

            @Override
            boolean execute(AerospikeStatement statement, String sql) throws SQLException {
                return executeUpdate(statement, sql) > 0;
            }
        },
        SHOW,
        ;


        ResultSet executeQuery(AerospikeStatement statement, String sql) throws SQLException {
            throw new UnsupportedOperationException(String.format("%s does not support %s", name(), "executeQuery"));
        }
        int executeUpdate(AerospikeStatement statement, String sql) throws SQLException {
            throw new UnsupportedOperationException(String.format("%s does not support %s", name(), "executeUpdate"));
        }
        boolean execute(AerospikeStatement statement, String sql) throws SQLException {
            throw new UnsupportedOperationException(String.format("%s does not support %s", name(), "execute"));
        }
    }

    public AerospikeStatement(IAerospikeClient client, Connection connection, String schema, AerospikePolicyProvider policyProvider) {
        this.client = client;
        this.connection = connection;
        this.schema = schema;
        this.policyProvider = policyProvider;
        indexes = parametersParser.indexesParser(Info.request(client.getNodes()[0], "sindex"));
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return getStatementType(sql).executeQuery(this, sql);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return getStatementType(sql).executeUpdate(this, sql);
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 8 * 1024 * 1024; //8 MB - the Aerospike limitation
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throw new UnsupportedOperationException("Max field size cannot be changed dynamically");
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

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return getStatementType(sql).execute(this, sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return 0;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return 0;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return 0;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return 0;
    }

    @Override
    public void addBatch(String sql) throws SQLException {

    }

    @Override
    public void clearBatch() throws SQLException {

    }

    @Override
    public int[] executeBatch() throws SQLException {
        return new int[0];
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
        return null;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return 0;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return false;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    private static StatementType getStatementType(String sql) {
        return StatementType.valueOf(sql.trim().split("\\s+")[0].toUpperCase());
    }
}
