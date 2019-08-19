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
import com.nosqldriver.VisibleForPackage;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

/**
 * Purpose of this class is to transform Aerospike exception into SQLExceptions and perform some validation
 * necessary for relational DBs and not performed by Aerospike.
 */
public class AerospikeSqlClient implements IAerospikeClient {
    private final IAerospikeClient client;

    @VisibleForPackage
    AerospikeSqlClient(IAerospikeClient client) {
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
        return client.getNode(nodeName);
    }

    @Override
    public ClusterStats getClusterStats() {
        try {
            return client.getClusterStats();
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.put(policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void put(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.put(eventLoop, listener, policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.append(policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void append(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.append(eventLoop, listener, policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.prepend(policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void prepend(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.prepend(eventLoop, listener, policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.add(policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void add(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
        try {
            client.add(eventLoop, listener, policy, key, bins);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
        try {
            return client.delete(policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return false; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void delete(EventLoop eventLoop, DeleteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        try {
            client.delete(eventLoop, listener, policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void truncate(InfoPolicy policy, String ns, String set, Calendar beforeLastUpdate) throws AerospikeException {
        try {
            client.truncate(policy, ns, set, beforeLastUpdate);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void touch(WritePolicy policy, Key key) throws AerospikeException {
        try {
            client.touch(policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void touch(EventLoop eventLoop, WriteListener listener, WritePolicy policy, Key key) throws AerospikeException {
        try {
            client.touch(eventLoop, listener, policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public boolean exists(Policy policy, Key key) throws AerospikeException {
        try {
            return client.exists(policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return false; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsListener listener, Policy policy, Key key) throws AerospikeException {
        try {
            client.exists(eventLoop, listener, policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            return client.exists(policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return new boolean[0]; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            client.exists(eventLoop, listener, policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void exists(EventLoop eventLoop, ExistsSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            client.exists(eventLoop, listener, policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record get(Policy policy, Key key) throws AerospikeException {
        try {
            return client.get(policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
        try {
            return client.get(policy, key, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordListener listener, Policy policy, Key key, String... binNames) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, key, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record getHeader(Policy policy, Key key) throws AerospikeException {
        try {
            return client.getHeader(policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordListener listener, Policy policy, Key key) throws AerospikeException {
        try {
            client.getHeader(eventLoop, listener, policy, key);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void get(BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        try {
            client.get(policy, records);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void get(EventLoop eventLoop, BatchListListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, records);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void get(EventLoop eventLoop, BatchSequenceListener listener, BatchPolicy policy, List<BatchRead> records) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, records);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            return client.get(policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        try {
            return client.get(policy, keys, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, keys, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void get(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
        try {
            client.get(eventLoop, listener, policy, keys, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            return client.getHeader(policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordArrayListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            client.getHeader(eventLoop, listener, policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void getHeader(EventLoop eventLoop, RecordSequenceListener listener, BatchPolicy policy, Key[] keys) throws AerospikeException {
        try {
            client.getHeader(eventLoop, listener, policy, keys);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        try {
            return client.operate(policy, key, operations);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void operate(EventLoop eventLoop, RecordListener listener, WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
        try {
            client.operate(eventLoop, listener, policy, key, operations);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        try {
            client.scanAll(policy, namespace, setName, callback, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void scanAll(EventLoop eventLoop, RecordSequenceListener listener, ScanPolicy policy, String namespace, String setName, String... binNames) throws AerospikeException {
        try {
            client.scanAll(eventLoop, listener, policy, namespace, setName, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        try {
            client.scanNode(policy, nodeName, namespace, setName, callback, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {
        try {
            client.scanNode(policy, node, namespace, setName, callback, binNames);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) throws AerospikeException {
        try {
            return client.register(policy, clientPath, serverPath, language);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language) throws AerospikeException {
        try {
            return client.register(policy, resourceLoader, resourcePath, serverPath, language);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public RegisterTask registerUdfString(Policy policy, String code, String serverPath, Language language) throws AerospikeException {
        try {
            return client.registerUdfString(policy, code, serverPath, language);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void removeUdf(InfoPolicy policy, String serverPath) throws AerospikeException {
        try {
            client.removeUdf(policy, serverPath);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
        try {
            return client.execute(policy, key, packageName, functionName, args);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void execute(EventLoop eventLoop, ExecuteListener listener, WritePolicy policy, Key key, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        try {
            client.execute(eventLoop, listener, policy, key, packageName, functionName, functionArgs);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        try {
            return client.execute(policy, statement, packageName, functionName, functionArgs);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
        try {
            return client.query(policy, validate(statement));
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void query(EventLoop eventLoop, RecordSequenceListener listener, QueryPolicy policy, Statement statement) throws AerospikeException {
        try {
            client.query(eventLoop, listener, policy, statement);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        try {
            return client.queryNode(policy, statement, node);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
        try {
            return client.queryAggregate(policy, statement, packageName, functionName, functionArgs);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
        try {
            return client.queryAggregate(policy, validate(statement));
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public ResultSet queryAggregateNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
        try {
            return client.queryAggregateNode(policy, validate(statement), node);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType) throws AerospikeException {
        try {
            return client.createIndex(policy, namespace, setName, indexName, binName, indexType);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {
        try {
            return client.createIndex(policy, namespace, setName, indexName, binName, indexType, indexCollectionType);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public IndexTask dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException {
        try {
            return client.dropIndex(policy, namespace, setName, indexName);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {
        try {
            client.createUser(policy, user, password, roles);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void dropUser(AdminPolicy policy, String user) throws AerospikeException {
        try {
            client.dropUser(policy, user);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {
        try {
            client.changePassword(policy, user, password);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        try {
            client.grantRoles(policy, user, roles);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {
        try {
            client.revokeRoles(policy, user, roles);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        try {
            client.createRole(policy, roleName, privileges);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {
        try {
            client.dropRole(policy, roleName);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        try {
            client.grantPrivileges(policy, roleName, privileges);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {
        try {
            client.revokePrivileges(policy, roleName, privileges);
        } catch (AerospikeException e) {
            throwSqlException(e);
        }
    }

    @Override
    public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
        try {
            return client.queryUser(policy, user);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
        try {
            return client.queryUsers(policy);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
        try {
            return client.queryRole(policy, roleName);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @Override
    public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
        try {
            return client.queryRoles(policy);
        } catch (AerospikeException e) {
            throwSqlException(e);
            return null; // just to satisfy compiler. The exception is thrown in the previous line.
        }
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }

    private SQLException sqlException(AerospikeException ae) {
        //TODO: implement sqlState (the second argument). see javadoc, https://stackoverflow.com/questions/14404866/how-to-detect-the-sql-error-state-of-sqlexception and http://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt for reference
        return new SQLException(ae.getMessage(), "", ae.getResultCode(), ae);
    }

    private void throwSqlException(AerospikeException ae) {
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

}
