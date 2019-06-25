package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Language;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.nosqldriver.VisibleForPackage;

import static com.nosqldriver.aerospike.sql.TestDataUtils.SUBJECT_SELECTION;
import static com.nosqldriver.aerospike.sql.TestDataUtils.client;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeAllPersonalInstruments;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeBeatles;
import static com.nosqldriver.aerospike.sql.TestDataUtils.writeSubjectSelection;

/**
 * This is not a real test by rather playground needed for temporary tests and exercises.
 * This class will be removed once driver becomes stable.
 */
public class DevTest {
    //@Test
    @VisibleForPackage
    void testFunction() {
        writeBeatles();
        writeSubjectSelection();
        Statement statement = new Statement();
        statement.setSetName("people");
        statement.setNamespace("test");
        //statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "add_stat_ops");
        //statement.setAggregateFunction(getClass().getClassLoader(), "sum1.lua", "sum1", "sum_single_bin", new Value.StringValue("year_of_birth"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "sum2.lua", "sum2", "sum_single_bin", new Value.StringValue("year_of_birth"));

        //System.setProperty("lua.dir", "/tmp");
        //statement.setAggregateFunction("sum2", "sum_single_bin", new Value.StringValue("year_of_birth"));

        //statement.setAggregateFunction(getClass().getClassLoader(), "sum3.lua", "sum3", "sum_single_bin", new Value.StringValue("year_of_birth"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "single_bin_stats", new Value.StringValue("year_of_birth"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "single_bin_stats");

        //client.register(new Policy(), getClass().getClassLoader(), "stats.lua", "stats.lua", Language.LUA);
        //client.register(new Policy(), getClass().getClassLoader(), "sum1.lua", "sum1.lua", Language.LUA).waitTillComplete();
        //client.register(new Policy(), getClass().getClassLoader(), "sum2.lua", "sum2.lua", Language.LUA).waitTillComplete();

//        statement.setAggregateFunction(getClass().getClassLoader(), "distinct.lua", "distinct", "distinct", new Value.StringValue("year_of_birth"));
//        client.register(new Policy(), getClass().getClassLoader(), "distinct.lua", "distinct.lua", Language.LUA);


        //statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("year_of_birth"), new Value.StringValue("avg:kids_count"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("groupby:year_of_birth"), new Value.StringValue("count:kids_count"));
        statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("groupby:year_of_birth"), new Value.StringValue("count:kids_count"), new Value.StringValue("sum:kids_count"));

        //statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("groupby:year_of_birth"), new Value.StringValue("max:kids_count"), new Value.StringValue("count:kids_count"));
        //statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("groupby:year_of_birth"), new Value.StringValue("max:kids_count"));
        client.register(new Policy(), getClass().getClassLoader(), "groupby.lua", "groupby.lua", Language.LUA);


        com.aerospike.client.query.ResultSet rs = client.queryAggregate(null, statement);
        while(rs.next()) {
            System.out.println("rec: " + rs.getObject());
        }
    }


    //@Test
    @VisibleForPackage
    void testFunction2() {
        writeSubjectSelection();


        Statement select = new Statement();
        select.setSetName(SUBJECT_SELECTION);
        select.setNamespace("test");
        RecordSet rs1 = client.query(new QueryPolicy(), select);
        while (rs1.next()) {
            System.out.println(rs1.getRecord().bins);
        }


        Statement groupBy = new Statement();
        groupBy.setSetName(SUBJECT_SELECTION);
        groupBy.setNamespace("test");

        //groupBy.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("groupby:subject"), new Value.StringValue("count"));
        groupBy.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", new Value.StringValue("groupby:subject"), new Value.StringValue("groupby:semester"), new Value.StringValue("count"));
        client.register(new Policy(), getClass().getClassLoader(), "groupby.lua", "groupby.lua", Language.LUA);


        com.aerospike.client.query.ResultSet rs = client.queryAggregate(null, groupBy);
        while(rs.next()) {
            System.out.println("rec: " + rs.getObject());
        }
    }


    //@Test
    @VisibleForPackage
    void select() {
        //writeMainPersonalInstruments();
        writeAllPersonalInstruments();
        Statement statement = new Statement();
        statement.setSetName("instruments");
        statement.setNamespace("test");
        //statement.setPredExp(PredExp.integerBin("person_id"), PredExp.integerValue(2), PredExp.integerEqual());
        //statement.setPredExp(PredExp.integerValue(2), PredExp.integerBin("person_id"), PredExp.integerEqual());


        //statement.setPredExp(PredExp.integerValue(2), PredExp.integerBin("id"), PredExp.integerEqual());
        //statement.setPredExp(PredExp.integerBin("person_id"), PredExp.integerBin("id"), PredExp.integerEqual());
        RecordSet rs = client.query(new QueryPolicy(), statement);
        while (rs.next()) {
            System.out.println(rs.getRecord().bins);
        }
    }

    //@Test
    @VisibleForPackage
    void fill() {
        writeBeatles();

        System.out.println(Info.request(new InfoPolicy(), client.getNodes()[0], "namespaces")); // test;bar
//        System.out.println(Info.request(new InfoPolicy(), client.getNodes()[0], "sets")); // ns=test:set=people:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=instruments:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=subject_selection:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=guitars:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=keyboards:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;

        //System.out.println(Info.request(new InfoPolicy(), client.getNodes()[0]));

//        for (Map.Entry<String, String> e : Info.request(new InfoPolicy(), client.getNodes()[0]).entrySet()) {
//            System.out.printf("key=%s, value=%s\n", e.getKey(), e.getValue());
//        }


        //String[] commands = {"build", "get-config", "logs", "namespaces", "namespace/test", "service", "services", "sets/test", "statistics"};
        //stream(commands).forEach(command -> {System.out.println("\n\n " + command + "\n-------------\n"); System.out.println(Info.request(new InfoPolicy(), client.getNodes()[0], command));});

        /*

 build
-------------

4.5.1.5


 get-config
-------------

paxos-single-replica-limit=1;pidfile=/var/run/aerospike/asd.pid;proto-fd-max=15000;advertise-ipv6=false;auto-pin=none;batch-index-threads=4;batch-max-buffers-per-queue=255;batch-max-requests=5000;batch-max-unused-buffers=256;cluster-name=null;enable-benchmarks-fabric=false;enable-benchmarks-svc=false;enable-health-check=false;enable-hist-info=false;feature-key-file=/etc/aerospike/features.conf;hist-track-back=300;hist-track-slice=10;hist-track-thresholds=null;info-threads=16;keep-caps-ssd-health=false;log-local-time=false;log-millis=false;migrate-fill-delay=0;migrate-max-num-incoming=4;migrate-threads=1;min-cluster-size=1;node-id=BB9F89BC50F5E18;node-id-interface=null;proto-fd-idle-ms=60000;proto-slow-netio-sleep-ms=1;query-batch-size=100;query-buf-size=2097152;query-bufpool-size=256;query-in-transaction-thread=false;query-long-q-max-size=500;query-microbenchmark=false;query-pre-reserve-partitions=false;query-priority=10;query-priority-sleep-us=1;query-rec-count-bound=18446744073709551615;query-req-in-query-thread=false;query-req-max-inflight=100;query-short-q-max-size=500;query-threads=6;query-threshold=10;query-untracked-time-ms=1000;query-worker-threads=15;run-as-daemon=true;scan-max-active=100;scan-max-done=100;scan-max-udf-transactions=32;scan-threads=4;service-threads=4;sindex-builder-threads=4;sindex-gc-max-rate=50000;sindex-gc-period=10;ticker-interval=10;transaction-max-ms=1000;transaction-queues=4;transaction-retry-ms=1002;transaction-threads-per-queue=4;work-directory=/opt/aerospike;debug-allocations=none;fabric-dump-msgs=false;service.port=3000;service.address=any;service.access-port=0;service.alternate-access-port=0;service.tls-port=0;service.tls-access-port=0;service.tls-alternate-access-port=0;service.tls-name=null;heartbeat.mode=multicast;heartbeat.multicast-group=239.1.99.222;heartbeat.port=9918;heartbeat.interval=150;heartbeat.timeout=10;heartbeat.mtu=1500;heartbeat.protocol=v3;fabric.port=3001;fabric.tls-port=0;fabric.tls-name=null;fabric.channel-bulk-fds=2;fabric.channel-bulk-recv-threads=4;fabric.channel-ctrl-fds=1;fabric.channel-ctrl-recv-threads=4;fabric.channel-meta-fds=1;fabric.channel-meta-recv-threads=4;fabric.channel-rw-fds=8;fabric.channel-rw-recv-threads=16;fabric.keepalive-enabled=true;fabric.keepalive-intvl=1;fabric.keepalive-probes=10;fabric.keepalive-time=1;fabric.latency-max-ms=5;fabric.recv-rearm-threshold=1024;fabric.send-threads=8;info.port=3003;enable-ldap=false;enable-security=false;ldap-login-threads=8;privilege-refresh-period=300;ldap.disable-tls=false;ldap.polling-period=300;ldap.query-base-dn=null;ldap.query-user-dn=null;ldap.query-user-password-file=null;ldap.role-query-base-dn=null;ldap.role-query-search-ou=false;ldap.server=null;ldap.session-ttl=86400;ldap.tls-ca-file=null;ldap.token-hash-method=sha-256;ldap.user-dn-pattern=null;ldap.user-query-pattern=null;report-authentication-sinks=0;report-data-op-sinks=0;report-sys-admin-sinks=0;report-user-admin-sinks=0;report-violation-sinks=0;syslog-local=-1


 logs
-------------

0:/var/log/aerospike/aerospike.log


 namespaces
-------------

test;bar


 namespace/test
-------------

ns_cluster_size=1;effective_replication_factor=1;objects=0;tombstones=0;master_objects=0;master_tombstones=0;prole_objects=0;prole_tombstones=0;non_replica_objects=0;non_replica_tombstones=0;dead_partitions=0;unavailable_partitions=0;clock_skew_stop_writes=false;stop_writes=false;hwm_breached=false;current_time=297979524;non_expirable_objects=0;expired_objects=0;evicted_objects=0;evict_ttl=0;evict_void_time=0;smd_evict_void_time=0;nsup_cycle_duration=0;truncate_lut=0;truncated_records=0;memory_used_bytes=0;memory_used_data_bytes=0;memory_used_index_bytes=0;memory_used_sindex_bytes=0;memory_free_pct=100;xmem_id=0;available_bin_names=32754;pending_quiesce=false;effective_is_quiesced=false;nodes_quiesced=0;effective_prefer_uniform_balance=false;migrate_tx_partitions_imbalance=0;migrate_tx_instances=0;migrate_rx_instances=0;migrate_tx_partitions_active=0;migrate_rx_partitions_active=0;migrate_tx_partitions_initial=0;migrate_tx_partitions_remaining=0;migrate_tx_partitions_lead_remaining=0;migrate_rx_partitions_initial=0;migrate_rx_partitions_remaining=0;migrate_records_skipped=0;migrate_records_transmitted=0;migrate_record_retransmits=0;migrate_record_receives=0;migrate_signals_active=0;migrate_signals_remaining=0;appeals_tx_active=0;appeals_rx_active=0;appeals_tx_remaining=0;appeals_records_exonerated=0;client_tsvc_error=0;client_tsvc_timeout=0;client_proxy_complete=0;client_proxy_error=0;client_proxy_timeout=0;client_read_success=77;client_read_error=0;client_read_timeout=0;client_read_not_found=2;client_write_success=1640;client_write_error=2;client_write_timeout=0;xdr_client_write_success=0;xdr_client_write_error=0;xdr_client_write_timeout=0;client_delete_success=1272;client_delete_error=0;client_delete_timeout=0;client_delete_not_found=0;xdr_client_delete_success=0;xdr_client_delete_error=0;xdr_client_delete_timeout=0;xdr_client_delete_not_found=0;client_udf_complete=0;client_udf_error=0;client_udf_timeout=0;client_lang_read_success=0;client_lang_write_success=0;client_lang_delete_success=0;client_lang_error=0;from_proxy_tsvc_error=0;from_proxy_tsvc_timeout=0;from_proxy_read_success=0;from_proxy_read_error=0;from_proxy_read_timeout=0;from_proxy_read_not_found=0;from_proxy_write_success=0;from_proxy_write_error=0;from_proxy_write_timeout=0;xdr_from_proxy_write_success=0;xdr_from_proxy_write_error=0;xdr_from_proxy_write_timeout=0;from_proxy_delete_success=0;from_proxy_delete_error=0;from_proxy_delete_timeout=0;from_proxy_delete_not_found=0;xdr_from_proxy_delete_success=0;xdr_from_proxy_delete_error=0;xdr_from_proxy_delete_timeout=0;xdr_from_proxy_delete_not_found=0;from_proxy_udf_complete=0;from_proxy_udf_error=0;from_proxy_udf_timeout=0;from_proxy_lang_read_success=0;from_proxy_lang_write_success=0;from_proxy_lang_delete_success=0;from_proxy_lang_error=0;batch_sub_tsvc_error=0;batch_sub_tsvc_timeout=0;batch_sub_proxy_complete=0;batch_sub_proxy_error=0;batch_sub_proxy_timeout=0;batch_sub_read_success=17;batch_sub_read_error=0;batch_sub_read_timeout=0;batch_sub_read_not_found=72;from_proxy_batch_sub_tsvc_error=0;from_proxy_batch_sub_tsvc_timeout=0;from_proxy_batch_sub_read_success=0;from_proxy_batch_sub_read_error=0;from_proxy_batch_sub_read_timeout=0;from_proxy_batch_sub_read_not_found=0;udf_sub_tsvc_error=0;udf_sub_tsvc_timeout=0;udf_sub_udf_complete=0;udf_sub_udf_error=0;udf_sub_udf_timeout=0;udf_sub_lang_read_success=0;udf_sub_lang_write_success=0;udf_sub_lang_delete_success=0;udf_sub_lang_error=0;retransmit_all_read_dup_res=0;retransmit_all_write_dup_res=0;retransmit_all_write_repl_write=0;retransmit_all_delete_dup_res=0;retransmit_all_delete_repl_write=0;retransmit_all_udf_dup_res=0;retransmit_all_udf_repl_write=0;retransmit_all_batch_sub_dup_res=0;retransmit_udf_sub_dup_res=0;retransmit_udf_sub_repl_write=0;scan_basic_complete=1467;scan_basic_error=8;scan_basic_abort=0;scan_aggr_complete=23;scan_aggr_error=0;scan_aggr_abort=0;scan_udf_bg_complete=0;scan_udf_bg_error=0;scan_udf_bg_abort=0;query_reqs=31;query_fail=0;query_short_queue_full=0;query_long_queue_full=0;query_short_reqs=31;query_long_reqs=0;query_agg=0;query_agg_success=0;query_agg_error=0;query_agg_abort=0;query_agg_avg_rec_count=0;query_lookups=31;query_lookup_success=31;query_lookup_error=0;query_lookup_abort=0;query_lookup_avg_rec_count=1;query_udf_bg_success=0;query_udf_bg_failure=0;geo_region_query_reqs=0;geo_region_query_cells=0;geo_region_query_points=0;geo_region_query_falsepos=0;re_repl_success=0;re_repl_error=0;re_repl_timeout=0;fail_xdr_forbidden=0;fail_key_busy=0;fail_generation=0;fail_record_too_big=0;deleted_last_bin=0;replication-factor=2;memory-size=4294967296;default-ttl=2592000;enable-xdr=false;sets-enable-xdr=true;ns-forward-xdr-writes=false;allow-nonxdr-writes=true;allow-xdr-writes=true;{test}-read-hist-track-back=300;{test}-read-hist-track-slice=10;{test}-read-hist-track-thresholds=1,8,64;{test}-query-hist-track-back=300;{test}-query-hist-track-slice=10;{test}-query-hist-track-thresholds=1,8,64;{test}-udf-hist-track-back=300;{test}-udf-hist-track-slice=10;{test}-udf-hist-track-thresholds=1,8,64;{test}-write-hist-track-back=300;{test}-write-hist-track-slice=10;{test}-write-hist-track-thresholds=1,8,64;conflict-resolution-policy=generation;data-in-index=false;disable-cold-start-eviction=false;disable-write-dup-res=false;disallow-null-setname=false;enable-benchmarks-batch-sub=false;enable-benchmarks-read=false;enable-benchmarks-udf=false;enable-benchmarks-udf-sub=false;enable-benchmarks-write=false;enable-hist-proxy=false;evict-hist-buckets=10000;evict-tenths-pct=5;high-water-disk-pct=50;high-water-memory-pct=60;index-stage-size=1073741824;index-type=undefined;migrate-order=5;migrate-retransmit-ms=5000;migrate-sleep=1;nsup-hist-period=3600;nsup-period=120;nsup-threads=1;partition-tree-sprigs=256;prefer-uniform-balance=false;rack-id=0;read-consistency-level-override=off;single-bin=false;stop-writes-pct=90;strong-consistency=false;strong-consistency-allow-expunge=false;tomb-raider-eligible-age=86400;tomb-raider-period=86400;transaction-pending-limit=20;write-commit-level-override=off;storage-engine=memory;sindex.num-partitions=32;geo2dsphere-within.strict=true;geo2dsphere-within.min-level=1;geo2dsphere-within.max-level=30;geo2dsphere-within.max-cells=12;geo2dsphere-within.level-mod=1;geo2dsphere-within.earth-radius-meters=6371000


 service
-------------

10.0.0.10:3000


 services
-------------




 sets/test
-------------

ns=test:set=people:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=instruments:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=subject_selection:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=guitars:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;ns=test:set=keyboards:objects=0:tombstones=0:memory_data_bytes=0:truncate_lut=0:stop-writes-count=0:set-enable-xdr=use-default:disable-eviction=false;


 statistics
-------------

cluster_size=1;cluster_key=C91AC4780FA5;cluster_generation=1;cluster_principal=BB9F89BC50F5E18;cluster_integrity=true;cluster_is_member=true;cluster_duplicate_nodes=null;cluster_clock_skew_stop_writes_sec=0;cluster_clock_skew_ms=0;cluster_clock_skew_outliers=null;uptime=171554;system_free_mem_pct=57;heap_allocated_kbytes=2165353;heap_active_kbytes=2168780;heap_mapped_kbytes=2252800;heap_efficiency_pct=96;heap_site_count=38;objects=0;tombstones=0;tsvc_queue=0;info_queue=0;rw_in_progress=0;proxy_in_progress=0;tree_gc_queue=0;client_connections=4;heartbeat_connections=0;fabric_connections=0;heartbeat_received_self=780456;heartbeat_received_foreign=0;reaped_fds=444;info_complete=67994;demarshal_error=0;early_tsvc_client_error=38;early_tsvc_from_proxy_error=0;early_tsvc_batch_sub_error=0;early_tsvc_from_proxy_batch_sub_error=0;early_tsvc_udf_sub_error=0;batch_index_initiate=69;batch_index_queue=0:0,0:0,0:0,0:0;batch_index_complete=69;batch_index_error=0;batch_index_timeout=0;batch_index_delay=0;batch_index_unused_buffers=1;batch_index_huge_buffers=0;batch_index_created_buffers=1;batch_index_destroyed_buffers=0;scans_active=0;query_short_running=0;query_long_running=0;sindex_ucgarbage_found=0;sindex_gc_retries=0;sindex_gc_list_creation_time=0;sindex_gc_list_deletion_time=0;sindex_gc_objects_validated=6;sindex_gc_garbage_found=0;sindex_gc_garbage_cleaned=0;paxos_principal=BB9F89BC50F5E18;time_since_rebalance=171552;migrate_allowed=true;migrate_partitions_remaining=0;fabric_bulk_send_rate=0;fabric_bulk_recv_rate=0;fabric_ctrl_send_rate=0;fabric_ctrl_recv_rate=0;fabric_meta_send_rate=0;fabric_meta_recv_rate=0;fabric_rw_send_rate=0;fabric_rw_recv_rate=0

         */
        System.out.println("done");
    }

    //@Test
    @VisibleForPackage
    void indexes() {
        //client.dropIndex(new Policy(), "bar", "people", "bar_people_id");
        //client.dropIndex(new Policy(), "bar", "people", "bar_people_first_name");
        //client.dropIndex(new Policy(), "test", "people", "test_people_year_of_birth");
//        client.createIndex(new Policy(), "bar", "people", "bar_people_id", "id", IndexType.NUMERIC);
//        client.createIndex(new Policy(), "bar", "people", "bar_people_first_name", "first_name", IndexType.STRING);
//        client.createIndex(new Policy(), "test", "people", "test_people_year_of_birth", "year_of_birth", IndexType.NUMERIC);
        fill();
        request("sindex-list:");
        request("sindex/test");
        request("sindex/bar");
    }


    private void request(String command) {
        Node node = client.getNodes()[0];
        System.out.println(command + ": " + Info.request(node, command));
    }

    //@Test
    @VisibleForPackage
    void writeToBar() {
        TestDataUtils.write(new WritePolicy(), new Key("bar", "people2", 1), TestDataUtils.person(1, "John", "Lennon", 1940, 2));
    }

}
