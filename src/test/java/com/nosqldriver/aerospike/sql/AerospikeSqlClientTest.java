package com.nosqldriver.aerospike.sql;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Record;
import com.aerospike.client.ScanCallback;
import com.aerospike.client.Value;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.async.EventLoop;
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
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.List;

import static com.nosqldriver.aerospike.sql.TestDataUtils.NAMESPACE;
import static com.nosqldriver.aerospike.sql.TestDataUtils.PEOPLE;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AerospikeSqlClientTest {
    @Test
    void successfulInit() {
        assertTrue(new AerospikeSqlClient(() -> TestDataUtils.client).isConnected());
    }

    @Test
    void unsuccessfulInitWrongHost() {
        assertThrows(SQLException.class, () -> new AerospikeSqlClient(() -> new AerospikeClient("wronghost", 3000)));
    }

    @Test
    void unsuccessfulInitWrongPort() {
        assertThrows(SQLException.class, () -> new AerospikeSqlClient(() -> new AerospikeClient("localhost", 3456)));
    }

    @Test
    void simpleGetters() {
        IAerospikeClient realClient = TestDataUtils.client;
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> realClient);
        assertEquals(realClient.getReadPolicyDefault(), wrapperClient.getReadPolicyDefault());
        assertEquals(realClient.getWritePolicyDefault(), wrapperClient.getWritePolicyDefault());
        assertEquals(realClient.getScanPolicyDefault(), wrapperClient.getScanPolicyDefault());
        assertEquals(realClient.getQueryPolicyDefault(), wrapperClient.getQueryPolicyDefault());
        assertEquals(realClient.getBatchPolicyDefault(), wrapperClient.getBatchPolicyDefault());
        assertEquals(realClient.getInfoPolicyDefault(), wrapperClient.getInfoPolicyDefault());

        assertEquals(realClient.isConnected(), wrapperClient.isConnected());
        assertArrayEquals(realClient.getNodes(), wrapperClient.getNodes());
        assertEquals(realClient.getNodeNames(), wrapperClient.getNodeNames());

        for (String nodeName : realClient.getNodeNames()) {
            assertEquals(realClient.getNode(nodeName), wrapperClient.getNode(nodeName));
        }

        // ClusterStats does not implement equals but implements consistent toString that can be used for comparison
        assertEquals(realClient.getClusterStats().toString(), wrapperClient.getClusterStats().toString());

        Key notExistingKey = new Key("test", "people", "does not exist");
        assertEquals(realClient.exists(null, notExistingKey), wrapperClient.exists(null, notExistingKey));
        assertArrayEquals(realClient.exists(null, new Key[] {notExistingKey}), wrapperClient.exists(null, new Key[] {notExistingKey}));
    }

    @Test
    void parameterizedGetters() {
        IAerospikeClient mock = mock(IAerospikeClient.class);
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> mock);
        Policy p = new Policy();
        BatchPolicy bp = new BatchPolicy();
        WritePolicy wp = new WritePolicy();
        QueryPolicy qp = new QueryPolicy();
        AdminPolicy ap = new AdminPolicy();

        Key key = new Key(NAMESPACE, PEOPLE, "KEY");
        Key[] keys = new Key[] {key};
        String[] bins = {"bin1", "bin2"};

        Record r = new Record(emptyMap(), 0, 0);
        Record[] rs = new Record[] {r};
        when(mock.get(p, key, bins)).thenReturn(r);
        assertEquals(r, wrapperClient.get(p, key, bins));

        when(mock.get(bp, keys, bins)).thenReturn(rs);
        assertEquals(rs, wrapperClient.get(bp, keys, bins));

        when(mock.getHeader(p, key)).thenReturn(r);
        assertEquals(r, wrapperClient.getHeader(p, key));

        when(mock.getHeader(bp, keys)).thenReturn(rs);
        assertEquals(rs, wrapperClient.getHeader(bp, keys));

        when(mock.operate(wp, key)).thenReturn(r);
        assertEquals(r, wrapperClient.operate(wp, key));


        RegisterTask registerTask = new RegisterTask(null, p, "package");
        when(mock.register(p, "clientpath", "serverpath", Language.LUA)).thenReturn(registerTask);
        assertEquals(registerTask, wrapperClient.register(p, "clientpath", "serverpath", Language.LUA));

        when(mock.registerUdfString(p, "code", "serverpath", Language.LUA)).thenReturn(registerTask);
        assertEquals(registerTask, wrapperClient.registerUdfString(p, "code", "serverpath", Language.LUA));

        when(mock.execute(wp, key, "package", "function")).thenReturn("ok");
        assertEquals("ok", wrapperClient.execute(wp, key, "package", "function"));


        Statement statement = new Statement();
        statement.setNamespace("test");
        statement.setSetName("set");
        ExecuteTask executeTask = new ExecuteTask(null, p, statement);
        when(mock.execute(wp, statement, "package", "function")).thenReturn(executeTask);
        assertEquals(executeTask, wrapperClient.execute(wp, statement, "package", "function"));

        Node node = mock(Node.class);
        when(mock.queryNode(qp, statement, node)).thenReturn(null);
        assertNull(wrapperClient.queryNode(qp, statement, node));

        when(mock.queryAggregate(qp, statement, "p", "f")).thenReturn(null);
        assertNull(wrapperClient.queryAggregate(qp, statement, "p", "f"));


        when(mock.queryAggregateNode(qp, statement, node)).thenReturn(null);
        assertNull(wrapperClient.queryAggregateNode(qp, statement, node));

        IndexTask it = new IndexTask(null, p, "test", "test_set_index", false);
        when(mock.createIndex(p, "test", "set", "test_set_index", "bin", IndexType.NUMERIC)).thenReturn(it);
        assertEquals(it, wrapperClient.createIndex(p, "test", "set", "test_set_index", "bin", IndexType.NUMERIC));

        User user = new User();
        when(mock.queryUser(ap, "user")).thenReturn(user);
        assertEquals(user, wrapperClient.queryUser(ap, "user"));


        List<User> users = singletonList(user);
        when(mock.queryUsers(ap)).thenReturn(users);
        assertEquals(users, wrapperClient.queryUsers(ap));


        Role role = new Role();
        when(mock.queryRole(ap, "role")).thenReturn(role);
        assertEquals(role, wrapperClient.queryRole(ap, "role"));


        List<Role> roles = singletonList(role);
        when(mock.queryRoles(ap)).thenReturn(roles);
        assertEquals(roles, wrapperClient.queryRoles(ap));
    }



    @Test
    void callOnClosedClient() {
        IAerospikeClient realClient = new AerospikeClient("localhost", 3000); // This test closes client, so it needs its own instance to avoid failures of other tests
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> realClient);
        assertCallOnClosedClient(realClient, AerospikeException.class);
        assertCallOnClosedClient(wrapperClient, SQLException.class);
    }

    @Test
    void delegation() {
        IAerospikeClient mock = mock(IAerospikeClient.class);
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> mock);

        Key key = new Key(NAMESPACE, PEOPLE, "KEY");
        WritePolicy wp = new WritePolicy();
        InfoPolicy ip = new InfoPolicy();
        BatchPolicy bp = new BatchPolicy();
        Policy p = new Policy();
        ScanPolicy sp = new ScanPolicy();
        QueryPolicy qp = new QueryPolicy();
        AdminPolicy ap = new AdminPolicy();
        EventLoop eventLoop = mock(EventLoop.class);
        WriteListener writeListener = mock(WriteListener.class);
        DeleteListener deleteListener = mock(DeleteListener.class);
        ExistsListener existsListener = mock(ExistsListener.class);
        ExistsSequenceListener existsSequenceListener = mock(ExistsSequenceListener.class);
        ExistsArrayListener existsArrayListener = mock(ExistsArrayListener.class);
        RecordListener recordListener = mock(RecordListener.class);
        BatchSequenceListener batchSequenceListener = mock(BatchSequenceListener.class);
        RecordArrayListener recordArrayListener = mock(RecordArrayListener.class);
        RecordSequenceListener recordSequenceListener = mock(RecordSequenceListener.class);
        ExecuteListener executeListener = mock(ExecuteListener.class);
        BatchListListener batchListListener = mock(BatchListListener.class);

        wrapperClient.put(wp, key);
        verify(mock, times(1)).put(wp, key);

        wrapperClient.put(eventLoop, writeListener, wp, key);
        verify(mock, times(1)).put(eventLoop, writeListener, wp, key);

        wrapperClient.append(wp, key);
        verify(mock, times(1)).append(wp, key);

        wrapperClient.append(eventLoop, writeListener, wp, key);
        verify(mock, times(1)).append(eventLoop, writeListener, wp, key);

        wrapperClient.prepend(wp, key);
        verify(mock, times(1)).prepend(wp, key);

        wrapperClient.prepend(eventLoop, writeListener, wp, key);
        verify(mock, times(1)).prepend(eventLoop, writeListener, wp, key);

        wrapperClient.add(wp, key);
        verify(mock, times(1)).add(wp, key);

        wrapperClient.add(eventLoop, writeListener, wp, key);
        verify(mock, times(1)).add(eventLoop, writeListener, wp, key);

        wrapperClient.delete(wp, key);
        verify(mock, times(1)).delete(wp, key);

        wrapperClient.delete(eventLoop, deleteListener, wp, key);
        verify(mock, times(1)).delete(eventLoop, deleteListener, wp, key);

        Calendar calendar = Calendar.getInstance();
        wrapperClient.truncate(ip, "ns", "set", calendar);
        verify(mock, times(1)).truncate(ip, "ns", "set", calendar);


        wrapperClient.touch(wp, key);
        verify(mock, times(1)).touch(wp, key);

        wrapperClient.touch(eventLoop, writeListener, wp, key);
        verify(mock, times(1)).touch(eventLoop, writeListener, wp, key);

        wrapperClient.exists(wp, key);
        verify(mock, times(1)).exists(wp, key);

        wrapperClient.exists(eventLoop, existsListener, wp, key);
        verify(mock, times(1)).exists(eventLoop, existsListener, wp, key);

        Key[] keys = new Key[] {key};
        wrapperClient.exists(bp, keys);
        verify(mock, times(1)).exists(bp, keys);

        wrapperClient.exists(eventLoop, existsSequenceListener, bp, keys);
        verify(mock, times(1)).exists(eventLoop, existsSequenceListener, bp, keys);

        wrapperClient.exists(eventLoop, existsArrayListener, bp, keys);
        verify(mock, times(1)).exists(eventLoop, existsArrayListener, bp, keys);


        wrapperClient.get(eventLoop, recordListener, p, key);
        verify(mock, times(1)).get(eventLoop, recordListener, p, key);

        String[] bins = {"bin1", "bin2"};
        wrapperClient.get(eventLoop, recordListener, p, key, bins);
        verify(mock, times(1)).get(eventLoop, recordListener, p, key, bins);


        wrapperClient.get(wp, key);
        verify(mock, times(1)).get(wp, key);

        wrapperClient.get(eventLoop, batchSequenceListener, bp, emptyList());
        verify(mock, times(1)).get(eventLoop, batchSequenceListener, bp, emptyList());

        wrapperClient.get(eventLoop, recordArrayListener, bp, keys);
        verify(mock, times(1)).get(eventLoop, recordArrayListener, bp, keys);

        wrapperClient.get(eventLoop, recordArrayListener, bp, keys, bins);
        verify(mock, times(1)).get(eventLoop, recordArrayListener, bp, keys, bins);


        wrapperClient.getHeader(eventLoop, recordArrayListener, bp, keys);
        verify(mock, times(1)).getHeader(eventLoop, recordArrayListener, bp, keys);

        wrapperClient.getHeader(eventLoop, recordSequenceListener, bp, keys);
        verify(mock, times(1)).getHeader(eventLoop, recordSequenceListener, bp, keys);

        wrapperClient.getHeader(eventLoop, recordListener, p, key);
        verify(mock, times(1)).getHeader(eventLoop, recordListener, p, key);

        wrapperClient.operate(eventLoop, recordListener, wp, key);
        verify(mock, times(1)).operate(eventLoop, recordListener, wp, key);

        ScanCallback scanCallback = mock(ScanCallback.class);
        wrapperClient.scanAll(sp, "namespace", "set", scanCallback);
        verify(mock, times(1)).scanAll(sp, "namespace", "set", scanCallback);

        wrapperClient.scanAll(eventLoop, recordSequenceListener, sp, "namespace", "set");
        verify(mock, times(1)).scanAll(eventLoop, recordSequenceListener, sp, "namespace", "set");

        wrapperClient.scanNode(sp, "node", "namespace", "set", scanCallback);
        verify(mock, times(1)).scanNode(sp, "node", "namespace", "set", scanCallback);

        Node node = mock(Node.class);
        wrapperClient.scanNode(sp, node, "namespace", "set", scanCallback);
        verify(mock, times(1)).scanNode(sp, node, "namespace", "set", scanCallback);


        wrapperClient.removeUdf(ip, "path");
        verify(mock, times(1)).removeUdf(ip, "path");

        wrapperClient.execute(eventLoop, executeListener, wp, key, "package", "function");
        verify(mock, times(1)).execute(eventLoop, executeListener, wp, key, "package", "function");

        Statement statement = new Statement();
        wrapperClient.query(eventLoop, recordSequenceListener, qp, statement);
        verify(mock, times(1)).query(eventLoop, recordSequenceListener, qp, statement);


        wrapperClient.createUser(ap, "user", "password", emptyList());
        verify(mock, times(1)).createUser(ap, "user", "password", emptyList());

        wrapperClient.dropUser(ap, "user");
        verify(mock, times(1)).dropUser(ap, "user");

        wrapperClient.changePassword(ap, "user", "password");
        verify(mock, times(1)).changePassword(ap, "user", "password");

        wrapperClient.grantRoles(ap, "user", emptyList());
        verify(mock, times(1)).grantRoles(ap, "user", emptyList());

        wrapperClient.revokeRoles(ap, "user", emptyList());
        verify(mock, times(1)).revokeRoles(ap, "user", emptyList());

        wrapperClient.createRole(ap, "role", emptyList());
        verify(mock, times(1)).createRole(ap, "role", emptyList());

        wrapperClient.dropRole(ap, "role");
        verify(mock, times(1)).dropRole(ap, "role");

        wrapperClient.grantPrivileges(ap, "role", emptyList());
        verify(mock, times(1)).grantPrivileges(ap, "role", emptyList());

        wrapperClient.revokePrivileges(ap, "role", emptyList());
        verify(mock, times(1)).revokePrivileges(ap, "role", emptyList());

        wrapperClient.get(bp, emptyList());
        verify(mock, times(1)).get(bp, emptyList());

        wrapperClient.get(eventLoop, batchListListener, bp, emptyList());
        verify(mock, times(1)).get(bp, emptyList());

        wrapperClient.get(eventLoop, recordSequenceListener, bp, keys);
        verify(mock, times(1)).get(eventLoop, recordSequenceListener, bp, keys);

        wrapperClient.get(eventLoop, recordSequenceListener, bp, keys, bins);
        verify(mock, times(1)).get(eventLoop, recordSequenceListener, bp, keys, bins);
    }


    @Test
    void thrownAerospikeException() {
        exceptions(new AerospikeException(""), SQLException.class);
    }

    @Test
    void thrownRuntimeException() {
        exceptions(new RuntimeException(""), RuntimeException.class);
    }

    private void exceptions(Throwable internalException, Class<? extends Throwable> externalExceptionType) {
        IAerospikeClient mock = mock(IAerospikeClient.class);
        IAerospikeClient wrapperClient = new AerospikeSqlClient(() -> mock);

        Key key = new Key(NAMESPACE, PEOPLE, "KEY");

        Statement statement = new Statement();
        statement.setNamespace("test");
        statement.setSetName("data");

        doThrow(internalException).when(mock).put(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.put(new WritePolicy(), key));

        doThrow(internalException).when(mock).put(any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.put(new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).put(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.put(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key));

        doThrow(internalException).when(mock).append(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.append(new WritePolicy(), key));

        doThrow(internalException).when(mock).put(any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.put(new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).append(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.append(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key));

        doThrow(internalException).when(mock).append(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.append(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).prepend(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.prepend(new WritePolicy(), key));

        doThrow(internalException).when(mock).prepend(any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.prepend(new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).prepend(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.prepend(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key));

        doThrow(internalException).when(mock).prepend(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.prepend(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).add(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.add(new WritePolicy(), key));

        doThrow(internalException).when(mock).add(any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.add(new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).add(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.add(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key));

        doThrow(internalException).when(mock).add(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class), any());
        assertThrows(externalExceptionType, () -> wrapperClient.add(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key, new Bin("name", "value")));

        doThrow(internalException).when(mock).delete(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.delete(new WritePolicy(), key));

        doThrow(internalException).when(mock).delete(any(EventLoop.class), any(DeleteListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.delete(mock(EventLoop.class), mock(DeleteListener.class), new WritePolicy(), key));

        doThrow(internalException).when(mock).truncate(any(InfoPolicy.class), eq(NAMESPACE), eq(PEOPLE), any(Calendar.class));
        assertThrows(externalExceptionType, () -> wrapperClient.truncate(new InfoPolicy(), NAMESPACE, PEOPLE, Calendar.getInstance()));

        doThrow(internalException).when(mock).touch(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.touch(new WritePolicy(), key));

        doThrow(internalException).when(mock).touch(any(EventLoop.class), any(WriteListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.touch(mock(EventLoop.class), mock(WriteListener.class), new WritePolicy(), key));

        doThrow(internalException).when(mock).exists(any(Policy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.exists(new Policy(), key));

        doThrow(internalException).when(mock).exists(any(EventLoop.class), any(ExistsListener.class), any(Policy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.exists(mock(EventLoop.class), mock(ExistsListener.class), new Policy(), key));


        doThrow(internalException).when(mock).exists(any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.exists(new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).exists(any(EventLoop.class), any(ExistsArrayListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.exists(mock(EventLoop.class), mock(ExistsArrayListener.class), new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).exists(any(EventLoop.class), any(ExistsSequenceListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.exists(mock(EventLoop.class), mock(ExistsSequenceListener.class), new BatchPolicy(), new Key[] {key}));



        doThrow(internalException).when(mock).get(any(Policy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(new Policy(), key));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordListener.class), any(Policy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordListener.class), new Policy(), key));

        doThrow(internalException).when(mock).get(any(Policy.class), any(Key.class), any(String[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(new Policy(), key));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordListener.class), any(Policy.class), any(Key.class), any(String[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordListener.class), new Policy(), key));

        doThrow(internalException).when(mock).getHeader(any(Policy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.getHeader(new Policy(), key));

        doThrow(internalException).when(mock).getHeader(any(EventLoop.class), any(RecordListener.class), any(Policy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.getHeader(mock(EventLoop.class), mock(RecordListener.class), new Policy(), key));

        doThrow(internalException).when(mock).get(any(BatchPolicy.class), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.get(new BatchPolicy(), emptyList()));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(BatchListListener.class), any(BatchPolicy.class), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(BatchListListener.class), new BatchPolicy(), emptyList()));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(BatchSequenceListener.class), any(BatchPolicy.class), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(BatchSequenceListener.class), new BatchPolicy(), emptyList()));

        doThrow(internalException).when(mock).get(any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordArrayListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordArrayListener.class), new BatchPolicy(), new Key[0]));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordSequenceListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordSequenceListener.class), new BatchPolicy(), new Key[0]));

        doThrow(internalException).when(mock).get(any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordArrayListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordArrayListener.class), new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordArrayListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordArrayListener.class), new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordArrayListener.class), any(BatchPolicy.class), any(Key[].class), any(String[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordArrayListener.class), new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).get(any(EventLoop.class), any(RecordSequenceListener.class), any(BatchPolicy.class), any(Key[].class), any(String[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.get(mock(EventLoop.class), mock(RecordSequenceListener.class), new BatchPolicy(), new Key[] {key}));


        doThrow(internalException).when(mock).getHeader(any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.getHeader(new BatchPolicy(), new Key[] {key}));

        doThrow(internalException).when(mock).getHeader(any(EventLoop.class), any(RecordArrayListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.getHeader(mock(EventLoop.class), mock(RecordArrayListener.class), new BatchPolicy(), new Key[0]));

        doThrow(internalException).when(mock).getHeader(any(EventLoop.class), any(RecordSequenceListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.getHeader(mock(EventLoop.class), mock(RecordSequenceListener.class), new BatchPolicy(), new Key[0]));

        doThrow(internalException).when(mock).getHeader(any(EventLoop.class), any(RecordSequenceListener.class), any(BatchPolicy.class), any(Key[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.getHeader(mock(EventLoop.class), mock(RecordSequenceListener.class), new BatchPolicy(), new Key[0]));

        doThrow(internalException).when(mock).operate(any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.operate(new WritePolicy(), key));

        doThrow(internalException).when(mock).operate(any(EventLoop.class), any(RecordListener.class), any(WritePolicy.class), any(Key.class));
        assertThrows(externalExceptionType, () -> wrapperClient.operate(mock(EventLoop.class), mock(RecordListener.class), new WritePolicy(), key));


        doThrow(internalException).when(mock).scanAll(any(ScanPolicy.class), eq(NAMESPACE), eq(PEOPLE), any(ScanCallback.class));
        assertThrows(externalExceptionType, () -> wrapperClient.scanAll(new ScanPolicy(), NAMESPACE, PEOPLE, mock(ScanCallback.class)));

        doThrow(internalException).when(mock).scanAll(any(EventLoop.class), any(RecordSequenceListener.class), any(ScanPolicy.class), eq(NAMESPACE), eq(PEOPLE));
        assertThrows(externalExceptionType, () -> wrapperClient.scanAll(mock(EventLoop.class), mock(RecordSequenceListener.class), new ScanPolicy(), NAMESPACE, PEOPLE));

        doThrow(internalException).when(mock).scanNode(any(ScanPolicy.class), eq("node1"), eq(NAMESPACE),  eq(PEOPLE), any(ScanCallback.class));
        assertThrows(externalExceptionType, () -> wrapperClient.scanNode(new ScanPolicy(), "node1", NAMESPACE, PEOPLE, mock(ScanCallback.class)));

        doThrow(internalException).when(mock).scanNode(any(ScanPolicy.class), any(Node.class), eq(NAMESPACE), eq(PEOPLE), any(ScanCallback.class));
        assertThrows(externalExceptionType, () -> wrapperClient.scanNode(new ScanPolicy(), mock(Node.class), NAMESPACE, PEOPLE, mock(ScanCallback.class)));

        doThrow(internalException).when(mock).register(any(Policy.class), eq("clientPath"), eq("serverPath"), eq(Language.LUA));
        assertThrows(externalExceptionType, () -> wrapperClient.register(new Policy(), "clientPath", "serverPath", Language.LUA));


        doThrow(internalException).when(mock).register(any(Policy.class), eq(getClass().getClassLoader()), eq("resourcePath"), eq("serverPath"), eq(Language.LUA));
        assertThrows(externalExceptionType, () -> wrapperClient.register(new Policy(), getClass().getClassLoader(), "resourcePath", "serverPath", Language.LUA));


        doThrow(internalException).when(mock).registerUdfString(any(Policy.class), eq("code"), eq("serverPath"), eq(Language.LUA));
        assertThrows(externalExceptionType, () -> wrapperClient.registerUdfString(new Policy(), "code", "serverPath", Language.LUA));

        doThrow(internalException).when(mock).removeUdf(any(InfoPolicy.class), eq("serverPath"));
        assertThrows(externalExceptionType, () -> wrapperClient.removeUdf(new InfoPolicy(), "serverPath"));


        doThrow(internalException).when(mock).execute(any(WritePolicy.class), eq(key), eq("packageName"), eq("functionName"), any(Value[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.execute(new WritePolicy(), key, "packageName", "functionName"));

        doThrow(internalException).when(mock).execute(any(EventLoop.class), any(ExecuteListener.class), any(WritePolicy.class), eq(key), eq("packageName"), eq("functionName"), any(Value[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.execute(mock(EventLoop.class), mock(ExecuteListener.class), new WritePolicy(), key, "packageName", "functionName"));

        doThrow(internalException).when(mock).execute(any(WritePolicy.class), any(Statement.class),  eq("packageName"), eq("functionName"), any(Value[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.execute(new WritePolicy(), statement, "packageName", "functionName"));


        doThrow(internalException).when(mock).query(any(QueryPolicy.class), any(Statement.class));
        assertThrows(externalExceptionType, () -> wrapperClient.query(new QueryPolicy(), statement));

        doThrow(internalException).when(mock).query(any(EventLoop.class), any(RecordSequenceListener.class), any(QueryPolicy.class), any(Statement.class));
        assertThrows(externalExceptionType, () -> wrapperClient.query(mock(EventLoop.class), mock(RecordSequenceListener.class), new QueryPolicy(), new Statement()));


        doThrow(internalException).when(mock).queryNode(any(QueryPolicy.class), any(Statement.class), any(Node.class));
        assertThrows(externalExceptionType, () -> wrapperClient.queryNode(new QueryPolicy(), statement, mock(Node.class)));


        doThrow(internalException).when(mock).queryAggregate(any(QueryPolicy.class), any(Statement.class),  eq("packageName"), eq("functionName"), any(Value[].class));
        assertThrows(externalExceptionType, () -> wrapperClient.queryAggregate(new QueryPolicy(), statement, "packageName", "functionName"));


        doThrow(internalException).when(mock).queryAggregate(any(QueryPolicy.class), any(Statement.class));
        assertThrows(externalExceptionType, () -> wrapperClient.queryAggregate(new QueryPolicy(), statement));

        doThrow(internalException).when(mock).queryAggregateNode(any(QueryPolicy.class), any(Statement.class), any(Node.class));
        assertThrows(externalExceptionType, () -> wrapperClient.queryAggregateNode(new QueryPolicy(), statement, mock(Node.class)));

        doThrow(internalException).when(mock).createIndex(any(Policy.class), any(String.class), any(String.class), any(String.class), any(String.class), any(IndexType.class));
        assertThrows(externalExceptionType, () -> wrapperClient.createIndex(new Policy(), "test", "set", "test_set_index", "bin", IndexType.NUMERIC));

        doThrow(internalException).when(mock).createIndex(any(Policy.class), eq(NAMESPACE), eq(PEOPLE), eq("index1"), eq("bin1"), eq(IndexType.NUMERIC));
        assertThrows(externalExceptionType, () -> wrapperClient.createIndex(new Policy(), NAMESPACE, PEOPLE, "index1", "bin1", IndexType.NUMERIC));

        doThrow(internalException).when(mock).createIndex(any(Policy.class), eq(NAMESPACE), eq(PEOPLE), eq("index1"), eq("bin1"), eq(IndexType.NUMERIC), eq(IndexCollectionType.DEFAULT));
        assertThrows(externalExceptionType, () -> wrapperClient.createIndex(new Policy(), NAMESPACE, PEOPLE, "index1", "bin1", IndexType.NUMERIC, IndexCollectionType.DEFAULT));

        doThrow(internalException).when(mock).dropIndex(any(Policy.class), eq(NAMESPACE), eq(PEOPLE), eq("index1"));
        assertThrows(externalExceptionType, () -> wrapperClient.dropIndex(new Policy(), NAMESPACE, PEOPLE, "index1"));


        doThrow(internalException).when(mock).createUser(any(AdminPolicy.class), eq("user"), eq("password1"), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.createUser(new AdminPolicy(), "user", "password1", emptyList()));

        doThrow(internalException).when(mock).dropUser(any(AdminPolicy.class), eq("user"));
        assertThrows(externalExceptionType, () -> wrapperClient.dropUser(new AdminPolicy(), "user"));

        doThrow(internalException).when(mock).changePassword(any(AdminPolicy.class), eq("user"), eq("password2"));
        assertThrows(externalExceptionType, () -> wrapperClient.changePassword(new AdminPolicy(), "user", "password2"));


        doThrow(internalException).when(mock).grantRoles(any(AdminPolicy.class), eq("user"), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.grantRoles(new AdminPolicy(), "user", emptyList()));

        doThrow(internalException).when(mock).revokeRoles(any(AdminPolicy.class), eq("user"), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.revokeRoles(new AdminPolicy(), "user", emptyList()));

        doThrow(internalException).when(mock).createRole(any(AdminPolicy.class), eq("role1"), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.createRole(new AdminPolicy(), "role1", emptyList()));


        doThrow(internalException).when(mock).dropRole(any(AdminPolicy.class), eq("role1"));
        assertThrows(externalExceptionType, () -> wrapperClient.dropRole(new AdminPolicy(), "role1"));

        doThrow(internalException).when(mock).grantPrivileges(any(AdminPolicy.class), eq("role1"), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.grantPrivileges(new AdminPolicy(), "role1", emptyList()));

        doThrow(internalException).when(mock).revokePrivileges(any(AdminPolicy.class), eq("role1"), ArgumentMatchers.anyList());
        assertThrows(externalExceptionType, () -> wrapperClient.revokePrivileges(new AdminPolicy(), "role1", emptyList()));

        doThrow(internalException).when(mock).queryUser(any(AdminPolicy.class), eq("user1"));
        assertThrows(externalExceptionType, () -> wrapperClient.queryUser(new AdminPolicy(), "user1"));

        doThrow(internalException).when(mock).queryUsers(any(AdminPolicy.class));
        assertThrows(externalExceptionType, () -> wrapperClient.queryUsers(new AdminPolicy()));

        doThrow(internalException).when(mock).queryRole(any(AdminPolicy.class), eq("role1"));
        assertThrows(externalExceptionType, () -> wrapperClient.queryRole(new AdminPolicy(), "role1"));

        doThrow(internalException).when(mock).queryRoles(any(AdminPolicy.class));
        assertThrows(externalExceptionType, () -> wrapperClient.queryRoles(new AdminPolicy()));
    }



    private <T extends Throwable> void assertCallOnClosedClient(IAerospikeClient client, Class<T> exceptionType) {
        client.close();
        client.getNodeNames(); // No validation here because this method returns empty list on build machine and non empty list on my computer. I do not know why.
        Key notExistingKey = new Key("test", "people", "does not exist");
        assertThrows(exceptionType, () -> client.exists(null, notExistingKey));
        assertThrows(exceptionType, () -> client.exists(null, new Key[] {notExistingKey}));
    }
}