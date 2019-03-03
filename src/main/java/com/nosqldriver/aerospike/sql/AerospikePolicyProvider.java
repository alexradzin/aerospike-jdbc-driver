package com.nosqldriver.aerospike.sql;

import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;

import java.util.Properties;


public class AerospikePolicyProvider {
    private final Policy policy;
    private final QueryPolicy queryPolicy;
    private final BatchPolicy batchPolicy;
    private final ScanPolicy scanPolicy;
    private final WritePolicy writePolicy;
    private final InfoPolicy infoPolicy;

    private final ConnectionParametersParser parser = new ConnectionParametersParser();


    AerospikePolicyProvider(Properties props) {
        policy = parser.initProperties(new Policy(), parser.subProperties(props, "policy"));
        queryPolicy = parser.initProperties(new QueryPolicy(), parser.subProperties(props, "policy.query"));
        batchPolicy = parser.initProperties(new BatchPolicy(), parser.subProperties(props, "policy.batch"));
        scanPolicy = parser.initProperties(new ScanPolicy(), parser.subProperties(props, "policy.scan"));
        writePolicy = parser.initProperties(new WritePolicy(), parser.subProperties(props, "policy.write"));
        infoPolicy = parser.initProperties(new InfoPolicy(), parser.subProperties(props, "policy.info"));
    }

    public Policy getPolicy() {
        return policy;
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
