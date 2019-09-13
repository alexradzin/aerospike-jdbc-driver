package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRead;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
import com.aerospike.client.cluster.ClusterStats;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.listener.BatchListListener;
import com.aerospike.client.listener.BatchSequenceListener;
import com.aerospike.client.listener.DeleteListener;
import com.aerospike.client.listener.ExecuteListener;
import com.aerospike.client.listener.ExistsArrayListener;
import com.aerospike.client.listener.ExistsListener;
import com.aerospike.client.listener.ExistsSequenceListener;
import com.aerospike.client.listener.RecordArrayListener;
import com.aerospike.client.listener.RecordListener;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.AdminPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.nosqldriver.util.SneakyThrower.sneakyThrow;

/**
 * Purpose of this class is to transform Aerospike exception into SQLExceptions and perform some validation
 * necessary for relational DBs and not performed by Aerospike.
 */
public class AerospikeSqlClient implements IAerospikeClient {
    private final IAerospikeClient client;

    AerospikeSqlClient(Supplier<IAerospikeClient> clientSupplier) {
        this(new ExceptionAwareSupplier<>(clientSupplier).get());
    }

    private AerospikeSqlClient(IAerospikeClient client) {
        this.client = client;
    }


    @Override
    public Policy getReadPolicyDefault() {
        return client.getReadPolicyDefault();
    }

    @Override
    public WritePolicy getWritePolicyDefault() {
        return client.getWritePolicyDefault();
    }

    @Override
    public ScanPolicy getScanPolicyDefault() {
        return client.getScanPolicyDefault();
    }

    @Override
    public QueryPolicy getQueryPolicyDefault() {
        return client.getQueryPolicyDefault();
    }

    @Override
    public BatchPolicy getBatchPolicyDefault() {
        return client.getBatchPolicyDefault();
    }

    @Override
    public InfoPolicy getInfoPolicyDefault() {
        return client.getInfoPolicyDefault();
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public boolean isConnected() {
        return client.isConnected();
    }

    @Override
    public Node[] getNodes() {
        return client.getNodes();
    }

    @Override
    public List<String> getNodeNames() {
        return client.getNodeNames();
    }

    @Override
    public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
        return get(() -> client.getNode(nodeName));
    }

    @Override
    public ClusterStats getClusterStats() {
        return client.getClusterStats();
    }

    @Override
    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.put(policy, key, bins));
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.put(eventLoop, listener, policy, key, bins));
    }

    @Override
    public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.append(policy, key, bins));
    }

    @Override
    public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.append(eventLoop, listener, policy, key, bins));
    }

    @Override
    public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.prepend(policy, key, bins));
    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.prepend(eventLoop, listener, policy, key, bins));
    }

    @Override
    public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.add(policy, key, bins));
    }

    @Override
    public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        call(client -> client.add(eventLoop, listener, policy, key, bins));
    }

    @Override
    public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
        return get(() -> client.delete(policy, key));
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        call(client -> client.delete(eventLoop, listener, policy, key));
    }

    @Override
    public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) throws AerospikeException {
        call(client -> client.truncate(policy, ns, set, beforeLastUpdate));
    }

    @Override
    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        call(client -> client.touch(policy, key));
    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        call(client -> client.touch(eventLoop, listener, policy, key));
    }

    @Override
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        return get(() -> client.exists(policy, key));
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
        call(client -> client.exists(eventLoop, listener, policy, key));
    }

    @Override
    public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return get(()-> client.exists(policy, keys));
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        call(client -> client.exists(eventLoop, listener, policy, keys));
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        call(client -> client.exists(eventLoop, listener, policy, keys));
    }

    @Override
    public Record get(Policy policy, Key key) throws AerospikeException {
        return get(() -> client.get(policy, key));
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, key));
    }

    @Override
    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        return get(() -> client.get(policy, key, binNames));
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, key, binNames));
    }

    @Override
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        return get(() -> client.getHeader(policy, key));
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        call(client -> client.getHeader(eventLoop, listener, policy, key));
    }

    @Override
    public void get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        call(client -> client.get(policy, records));
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, records));
    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, records));
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return get(() -> client.get(policy, keys));
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, keys));
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, keys));
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        return get(() -> client.get(policy, keys, binNames));
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, keys, binNames));
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        call(client -> client.get(eventLoop, listener, policy, keys, binNames));
    }

    @Override
    public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
        return get(() -> client.getHeader(policy, keys));
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        call(client -> client.getHeader(eventLoop, listener, policy, keys));
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        call(client -> client.getHeader(eventLoop, listener, policy, keys));
    }

    @Override
    public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        return get(() -> client.operate(policy, key, operations));
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        call(client -> client.operate(eventLoop, listener, policy, key, operations));
    }

    @Override
    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        call(client -> client.scanAll(policy, namespace, setName, callback, binNames));
    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException {
        call(client -> client.scanAll(eventLoop, listener, policy, namespace, setName, binNames));
    }

    @Override
    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        call(client -> client.scanNode(policy, nodeName, namespace, setName, callback, binNames));
    }

    @Override
    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        call(client -> client.scanNode(policy, node, namespace, setName, callback, binNames));
    }

    @Override
    public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) throws AerospikeException {
        return get(() -> client.register(policy, clientPath, serverPath, language));
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language) throws AerospikeException {
        return get(() -> client.register(policy, resourceLoader, resourcePath, serverPath, language));
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) throws AerospikeException {
        return get(() -> client.registerUdfString(policy, code, serverPath, language));
    }

    @Override
    public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
        call(client -> client.removeUdf(policy, serverPath));
    }

    @Override
    public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
        return get(() -> client.execute(policy, key, packageName, functionName, args));
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        call(client -> client.execute(eventLoop, listener, policy, key, packageName, functionName, functionArgs));
    }

    @Override
    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        return get(() -> client.execute(policy, statement, packageName, functionName, functionArgs));
    }

    @Override
    public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
        return get(() -> client.query(policy, validate(statement)));
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) throws AerospikeException {
        call(client -> client.query(eventLoop, listener, policy, statement));
    }

    @Override
    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        return get(() -> client.queryNode(policy, statement, node));
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        return get(() -> client.queryAggregate(policy, statement, packageName, functionName, functionArgs));
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
        return get(() -> client.queryAggregate(policy, validate(statement)));
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        return get(() -> client.queryAggregateNode(policy, validate(statement), node));
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType) throws AerospikeException {
        return get(() -> client.createIndex(policy, namespace, setName, indexName, binName, indexType));
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {
        return get(() -> client.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType));
    }

    @Override
    public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException {
        return get(() -> client.dropIndex(policy, namespace, setName, indexName));
    }

    @Override
    public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
        call(client -> client.createUser(policy, user, password, roles));
    }

    @Override
    public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
        call(client -> client.dropUser(policy, user));
    }

    @Override
    public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
        call(client -> client.changePassword(policy, user, password));
    }

    @Override
    public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        call(client -> client.grantRoles(policy, user, roles));
    }

    @Override
    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        call(client -> client.revokeRoles(policy, user, roles));
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        call(client -> client.createRole(policy, roleName, privileges));
    }

    @Override
    public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
        call(client -> client.dropRole(policy, roleName));
    }

    @Override
    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        call(client -> client.grantPrivileges(policy, roleName, privileges));
    }

    @Override
    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        call(client -> client.revokePrivileges(policy, roleName, privileges));
    }

    @Override
    public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
        return get(() -> client.queryUser(policy, user));
    }

    @Override
    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        return get(() -> client.queryUsers(policy));
    }

    @Override
    public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
        return get(() -> client.queryRole(policy, roleName));
    }

    @Override
    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        return get(() -> client.queryRoles(policy));
    }

    private static SQLException sqlException(AerospikeException ae) {
        //TODO: implement sqlState (the second argument). see javadoc, https://stackoverflow.com/questions/14404866/how-to-detect-the-sql-error-state-of-sqlexception and http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt for reference
        return new SQLException(ae.getMessage(), "", ae.getResultCode(), ae);
    }

    private static void throwSqlException(AerospikeException ae) {
        sneakyThrow(sqlException(ae));
    }


    private Statement validate(Statement statement) {
        if (statement.getNamespace() == null) {
            sneakyThrow(new SQLException("Unknown schema"));
        }
        if (statement.getSetName() == null) {
            sneakyThrow(new SQLException("Table name is not specified"));
        }
        return statement;
    }

    static class ExceptionAwareSupplier<T> implements Supplier<T> {
        private final Supplier<T> supplier;

        ExceptionAwareSupplier(Supplier<T> supplier) {
            this.supplier = supplier;
        }

        @Override
        public T get() {
            try {
                return supplier.get();
            } catch (AerospikeException ae) {
                throwSqlException(ae);
                return null;
            }
        }
    }


    @SuppressWarnings("ConstantConditions") // In fact SQLException and RuntimeException are expected here
    private <R> R get(Supplier<R> c) {
        try {
            return c.get();
        } catch (Exception e) {
            if (e instanceof AerospikeException) {
                throwSqlException((AerospikeException) e);
            } else if (e instanceof SQLException) {
                sneakyThrow(e);
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            }
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("ConstantConditions") // In fact SQLException and RuntimeException are expected here
    private void call(Consumer<IAerospikeClient> c) {
        try {
            c.accept(client);
        } catch (Exception e) {
            if (e instanceof AerospikeException) {
                throwSqlException((AerospikeException) e);
            } else if (e instanceof SQLException) {
                sneakyThrow(e);
            } else if (e instanceof RuntimeException) {
                throw (RuntimeException)e;
            }
            throw new IllegalStateException(e);
        }
    }
}
