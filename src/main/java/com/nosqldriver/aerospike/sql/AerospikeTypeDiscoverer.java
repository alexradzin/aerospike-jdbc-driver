package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.TypeDiscoverer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.SqlLiterals.getSqlType;

public class AerospikeTypeDiscoverer implements TypeDiscoverer {
    private final IAerospikeClient client;
    private final int limit;

    public AerospikeTypeDiscoverer(IAerospikeClient client) {
        this(client, 1);
    }

    public AerospikeTypeDiscoverer(IAerospikeClient client, int limit) {
        this.client = client;
        this.limit = limit;
    }

    @Override
    public List<DataColumn> discoverType(List<DataColumn> columns) {
        boolean all = columns.size() == 1 && "*".equals(columns.get(0).getName());

        List<DataColumn> result = all ? new ArrayList<>() : columns;
        Map<String[], List<DataColumn>> columnsByTable = columns.stream().collect(Collectors.groupingBy(c -> new String[] {c.getCatalog(), c.getTable()}));
        for (Map.Entry<String[], List<DataColumn>> ctd : columnsByTable.entrySet()) {
            String[] ct = ctd.getKey();
            String catalog = ct[0];
            String table = ct[1];
            Statement statement = new Statement();
            statement.setNamespace(catalog);
            statement.setSetName(table);
            Iterator<KeyRecord> it = client.query(new QueryPolicy(), statement).iterator();
            for (int i = 0; it.hasNext() && i < limit; i++) {
                Record r = it.next().record;
                if (all) {
                    result.addAll(r.bins.entrySet().stream()
                            .map(e -> DATA.create(catalog, table, e.getKey(), e.getKey()).withType(getSqlType(e.getValue())))
                            .collect(Collectors.toList()));
                } else {
                    ctd.getValue().forEach(c -> {
                        Object value = r.getValue(c.getName());
                        if (value != null) {
                            c.withType(getSqlType(value));
                        }
                    });
                }
            }
        }

        return result;
    }
}
