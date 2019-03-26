package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Language;
import com.aerospike.client.policy.Policy;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;

public class AerospikeConnection implements Connection {
    private final String url;
    private final Properties props;
    private static final ConnectionParametersParser parser = new ConnectionParametersParser();
    private final IAerospikeClient client;
    private volatile boolean readOnly = false;
    private volatile SQLWarning sqlWarning;
    private volatile Map<String, Class<?>> typeMap = Collections.emptyMap();
    private volatile int holdability = HOLD_CURSORS_OVER_COMMIT;
    private final Properties clientInfo = new Properties();
    private volatile String schema;
    private final AerospikePolicyProvider policyProvider;

    public AerospikeConnection(String url, Properties props) {
        this.url = url;
        this.props = props;
        Host[] hosts = parser.hosts(url);
        client = new AerospikeClient(parser.policy(url, props), hosts);
        schema = parser.schema(url);
        policyProvider = new AerospikePolicyProvider(parser.clientInfo(url, props));

        registerScript("stats", "distinct", "groupby");
        getMetaData();
    }

    private void registerScript(String ... names) {
        Policy regPolicy = policyProvider.getPolicy();
        ClassLoader cl = getClass().getClassLoader();
        Arrays.stream(names).map(name -> name + ".lua")
                .forEach(script -> client.register(regPolicy, cl, script, script, Language.LUA)
                        .waitTillComplete());
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new AerospikeStatement(client, this, schema, policyProvider);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return null;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        // do nothing here.
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    @Override
    public void commit() throws SQLException {
        // do nothing here.
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException("Aerospike is not transactional, so rollback isnot supported");
    }

    @Override
    public void close() throws SQLException {
        client.close();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return !client.isConnected();
    }

    @Override
    public DatabaseMetaData getMetaData() {
        return new AerospikeDatabaseMetadata(url, props, client); //TODO First thing to implement!
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Cannot set read only mode on closed connection");
        }
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        // do nothing. Aerospike does not support catalogs
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {

    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return 0;
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return typeMap;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        this.typeMap = map;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (isClosed()) {
            throw new SQLException("Cannot set holdability on closed connection");
        }
        if (!(holdability == HOLD_CURSORS_OVER_COMMIT || holdability == CLOSE_CURSORS_AT_COMMIT)) {
            throw new SQLException(format(
                    "Unsupported holdability %d. Must be either HOLD_CURSORS_OVER_COMMIT=%d or CLOSE_CURSORS_AT_COMMIT=%d",
                    holdability,
                    HOLD_CURSORS_OVER_COMMIT,
                    CLOSE_CURSORS_AT_COMMIT));
        }

        this.holdability = holdability;
    }

    @Override
    public int getHoldability() throws SQLException {
        return holdability;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Connection is not transactional");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return client.isConnected() && client.getClusterStats() != null;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.setProperty(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException(); // TODO: implement in the next phase
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        executor.execute(() -> {
            try {
                close();
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Network timeout cannot be changed");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return client.getQueryPolicyDefault().totalTimeout;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wrapping is not supported");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Wrapping is not supported");
    }
}