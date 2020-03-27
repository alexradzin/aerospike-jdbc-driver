package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.util.Arrays;
import java.util.Properties;


public class AerospikePolicyProvider {
    private final Policy readPolicy;
    private final QueryPolicy queryPolicy;
    private final BatchPolicy batchPolicy;
    private final ScanPolicy scanPolicy;
    private final WritePolicy writePolicy;
    private final InfoPolicy infoPolicy;

    private final ConnectionParametersParser parser = new ConnectionParametersParser();


    AerospikePolicyProvider(IAerospikeClient client, Properties props) {
        Properties common = parser.subProperties(props, "policy.*");
        readPolicy = parser.initProperties(client.getReadPolicyDefault(), merge(common, parser.subProperties(props, "policy.read")));
        queryPolicy = parser.initProperties(client.getQueryPolicyDefault(), merge(common, parser.subProperties(props, "policy.query")));
        batchPolicy = parser.initProperties(client.getBatchPolicyDefault(), merge(common, parser.subProperties(props, "policy.batch")));
        scanPolicy = parser.initProperties(client.getScanPolicyDefault(), merge(common, parser.subProperties(props, "policy.scan")));
        writePolicy = parser.initProperties(client.getWritePolicyDefault(), merge(common, parser.subProperties(props, "policy.write")));
        infoPolicy = parser.initProperties(client.getInfoPolicyDefault(), merge(common, parser.subProperties(props, "policy.info")));
    }

    private Properties merge(Properties ... properties) {
        Properties result = new Properties();
        Arrays.stream(properties).forEach(result::putAll);
        return result;
    }

    public Policy getReadPolicy() {
        return readPolicy;
    }

    public QueryPolicy getQueryPolicy() {
        return queryPolicy;
    }

    public BatchPolicy getBatchPolicy() {
        return batchPolicy;
    }

    public ScanPolicy getScanPolicy() {
        return scanPolicy;
    }

    public WritePolicy getWritePolicy() {
        return writePolicy;
    }

    public InfoPolicy getInfoPolicy() {
        return infoPolicy;
    }
}
