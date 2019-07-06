package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ListRecordSet;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;

public class AerospikeInsertQuery extends AerospikeQuery<Iterable<List<Object>>, WritePolicy> {
    private final String set;
    private final int indexOfPK;
    private final boolean skipDuplicates;
    public final static ThreadLocal<Integer> updatedRecordsCount = new ThreadLocal<>();


    public AerospikeInsertQuery(String schema, String set, String[] names, List<DataColumn> columns, Iterable<List<Object>> data, WritePolicy policy, boolean skipDuplicates) {
        super(schema, names, columns, data, policy);
        this.set = set;
        this.skipDuplicates = skipDuplicates;
        Arrays.stream(names).filter("PK"::equals).findFirst().orElseThrow(() -> new IllegalArgumentException("PK is not specified"));

        indexOfPK =
                IntStream.range(0, names.length)
                        .filter(i -> "PK".equals(names[i]))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException("No PK in insert statement"));
    }

    @Override
    public ResultSet apply(IAerospikeClient client) {
        updatedRecordsCount.remove();

        if (!skipDuplicates) {
            List<Key> keys = new LinkedList<>();
            for (List<Object> row : criteria) {
                keys.add(key(row));
            }
            if (Arrays.stream(client.get(new BatchPolicy(), keys.toArray(new Key[0]))).anyMatch(Objects::nonNull)) {
                sneakyThrow(new SQLException("Duplicate entries"));
            }
        }

        int n = 0;
        for (List<Object> row : criteria) {
            client.put(policy, key(row), bins(row));
            n++;
        }

        updatedRecordsCount.set(n);

        return new ListRecordSet(schema, set, emptyList(), emptyList());
    }

    private Key key(List<Object> row) {
        Object pk = row.get(indexOfPK);
        if (pk instanceof String) {
            return new Key(schema, set, (String) pk);
        }
        if (pk instanceof Integer) {
            return new Key(schema, set, (Integer) pk);
        }
        if (pk instanceof Long) {
            return new Key(schema, set, (Long) pk);
        }
        if (pk instanceof byte[]) {
            return new Key(schema, set, (byte) pk);
        }

        throw new IllegalArgumentException("Key must be either String, int, long or byte[]. " + pk.getClass() + " is not supported");
    }


    private Bin[] bins(List<Object> row) {
        Bin[] bins = new Bin[names.length - 1];
        for (int i = 0, j = 0; i < names.length; i++) {
            if (i != indexOfPK) {
                bins[j] = new Bin(names[i], row.get(i));
                j++;
            }
        }

        return bins;
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }


}
