package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Language;
import com.aerospike.client.policy.Policy;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.sql.BasicArray;
import com.nosqldriver.sql.ByteArrayBlob;
import com.nosqldriver.sql.SimpleWrapper;
import com.nosqldriver.sql.StringClob;
import com.nosqldriver.sql.WarningsHolder;
import com.nosqldriver.util.FunctionManager;
import com.nosqldriver.util.SneakyThrower;

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
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyMap;

@VisibleForPackage
class AerospikeConnection extends WarningsHolder implements Connection, SimpleWrapper {
    private final String url;
    private final Properties props;
    private static final ConnectionParametersParser parser = new ConnectionParametersParser();
    private final IAerospikeClient client;
    private volatile boolean readOnly = false;
    private volatile Map<String, Class<?>> typeMap = emptyMap();
    private volatile int holdability = HOLD_CURSORS_OVER_COMMIT;
    private final Properties clientInfo = new Properties();
    private final AtomicReference<String> schema = new AtomicReference<>(null); // schema can be updated by use statement
    private final AerospikePolicyProvider policyProvider;
    private final KeyRecordFetcherFactory keyRecordFetcherFactory;
    private volatile AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final FunctionManager functionManager;
    private static final String CUSTOM_FUNCTION_PREFIX = "custom.function.";
    private static final int CUSTOM_FUNCTION_PREFIX_LENGTH = CUSTOM_FUNCTION_PREFIX.length();
    private final boolean getPk;

    @VisibleForPackage
    AerospikeConnection(String url, Properties props) {
        this.url = url;
        this.props = props;
        Host[] hosts = parser.hosts(url);
        Properties info = parser.clientInfo(url, props);
        client = new AerospikeSqlClient(() -> new AerospikeClient(parser.policy(url, props), hosts));
        schema.set(parser.schema(url));
        policyProvider = new AerospikePolicyProvider(client, info);
        keyRecordFetcherFactory = new KeyRecordFetcherFactory(policyProvider.getQueryPolicy());
        FunctionManager fm = new FunctionManager(getMetaData());
        functionManager = init(fm, info);
        registerScript("stats", "distinct", "groupby");
        getPk = Stream.of(policyProvider.getQueryPolicy(), policyProvider.getBatchPolicy(), policyProvider.getScanPolicy()).anyMatch(p -> p.sendKey);
    }

    private void registerScript(String ... names) {
        Policy regPolicy = policyProvider.getReadPolicy();
        ClassLoader cl = getClass().getClassLoader();
        stream(names).map(name -> name + ".lua").forEach(script -> client.register(regPolicy, cl, script, script, Language.LUA).waitTillComplete());
    }

    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("nativeSQL is not supported");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        boolean prevValue = this.autoCommit.getAndSet(autoCommit);
        if (prevValue != autoCommit && !autoCommit) {
            addWarning("Aerospike does not  support transactions and therefore behaves like autocommit ON");
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit.get();
    }

    @Override
    public void commit() throws SQLException {
        // do nothing here.
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Aerospike is not transactional, so rollback is not supported");
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
        return new AerospikeDatabaseMetadata(url, props, client, this, policyProvider, functionManager);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (!isValid(1)) {
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
        this.schema.set(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return schema.get();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (level != TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException(format("Aerospike does not support transactions, so the only valid value here is TRANSACTION_NONE=%d", TRANSACTION_NONE));
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_NONE;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, getHoldability());
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, getHoldability());
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
        validateResultSetParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new AerospikeStatement(client, this, schema, policyProvider, functionManager);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        validateResultSetParameters(resultSetType, resultSetConcurrency, resultSetHoldability);
        return new AerospikePreparedStatement(client, this, schema, policyProvider, sql, keyRecordFetcherFactory, functionManager, getPk);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall is not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (sql.trim().toUpperCase().startsWith("INSERT") && autoGeneratedKeys != NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        if (sql.trim().toUpperCase().startsWith("INSERT") && columnIndexes != null && columnIndexes.length > 0) {
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        if (sql.trim().toUpperCase().startsWith("INSERT") && columnNames != null && columnNames.length > 0) {
            throw new SQLFeatureNotSupportedException("Auto generated keys are not supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        return new StringClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return new ByteArrayBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return new StringClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML are not supported");
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
        return new BasicArray(schema.get(), typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException(); // TODO: implement in the next phase
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        // do nothing. Aerospike's namespace is mapped to catalog
    }

    @Override
    public String getSchema() throws SQLException {
        return null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        executor.execute(() -> {
            SneakyThrower.get(() -> {
                close();
                return null;
            });
        });
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        stream(new Policy[] {
                client.getReadPolicyDefault(),
                client.getWritePolicyDefault(),
                client.getScanPolicyDefault(),
                client.getQueryPolicyDefault(),
                client.getBatchPolicyDefault()
        }).forEach(p -> p.totalTimeout = milliseconds);
        client.getInfoPolicyDefault().timeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return client.getReadPolicyDefault().totalTimeout;
    }

    private void validateResultSetParameters(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetType != TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("ResultSet type other than TYPE_FORWARD_ONLY is not supported");
        }
        if (resultSetConcurrency != CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Updatable ResultSet is not supported yet");
        }
        if (!(resultSetHoldability == HOLD_CURSORS_OVER_COMMIT || resultSetHoldability == CLOSE_CURSORS_AT_COMMIT)) {
            throw new SQLException(format("Wrong value of resultSetHoldability (%d). Supported values are: HOLD_CURSORS_OVER_COMMIT=%d or CLOSE_CURSORS_AT_COMMIT=%d", resultSetHoldability, HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT));
        }
    }

    private FunctionManager init(FunctionManager functionManager, Properties props) {
        props.entrySet().stream()
                .filter(e -> ((String)e.getKey()).startsWith(CUSTOM_FUNCTION_PREFIX))
                .forEach(e -> functionManager.addFunction(((String)e.getKey()).substring(CUSTOM_FUNCTION_PREFIX_LENGTH), (String)e.getValue()));
        return functionManager;
    }
}