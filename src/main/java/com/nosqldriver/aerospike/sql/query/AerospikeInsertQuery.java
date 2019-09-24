package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ListRecordSet;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.nosqldriver.util.SneakyThrower.sneakyThrow;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

public class AerospikeInsertQuery extends AerospikeQuery<Iterable<List<Object>>, WritePolicy, Object> {
    private final int indexOfPK;
    private final boolean skipDuplicates;
    public final static ThreadLocal<Integer> updatedRecordsCount = new ThreadLocal<>();

    private final static Map<Predicate<Object>, Function<Object, Object>> valueTransformer = new LinkedHashMap<>();
    static {
        valueTransformer.put(Objects::isNull, o -> null);
        valueTransformer.put(o -> o != null && BigDecimal.class.equals(o.getClass()), o -> ((BigDecimal)o).doubleValue());
        valueTransformer.put(o -> o instanceof Blob, blob -> {
            try {
                return ((Blob)blob).getBytes(1, (int)((Blob)blob).length());
            } catch (SQLException e) {
                sneakyThrow(e);
                return null;
            }
        });
        valueTransformer.put(o -> o instanceof byte[], bytes -> bytes);
        valueTransformer.put(o -> o instanceof Clob, clob -> {
            try {
                return ((Clob)clob).getSubString(1, (int)((Clob)clob).length());
            } catch (SQLException e) {
                sneakyThrow(e);
                return null;
            }
        });


        valueTransformer.put(o -> o instanceof Array, arr -> {
            try {
                return values(Arrays.stream(((Object[]) ((Array) arr).getArray())));
                //return Arrays.stream(((Object[]) ((Array) arr).getArray())).map(AerospikeInsertQuery::binValue).collect(Collectors.toList());
                //return Arrays.asList((Object[])((Array)arr).getArray());
            } catch (SQLException e) {
                sneakyThrow(e);
                return null;
            }
        });

//        valueTransformer.put(o -> o instanceof Collection<?>, collection -> collection instanceof List ? collection : new LinkedList<Object>((Collection)collection).stream().map(AerospikeInsertQuery::binValue).collect(Collectors.toList()));



        valueTransformer.put(o -> o instanceof Collection<?>, collection -> values(((Collection<?>)collection).stream()));
        valueTransformer.put(o -> o.getClass().isArray() && !o.getClass().getComponentType().isPrimitive(), a -> values(Arrays.stream((Object[])a)));

        valueTransformer.put(o -> o.getClass().isArray() && int.class.equals(o.getClass().getComponentType()), a -> Arrays.stream((int[])a).boxed().collect(Collectors.toList()));
        valueTransformer.put(o -> o.getClass().isArray() && long.class.equals(o.getClass().getComponentType()), a -> Arrays.stream((long[])a).boxed().collect(Collectors.toList()));
        valueTransformer.put(o -> o.getClass().isArray() && double.class.equals(o.getClass().getComponentType()), a -> Arrays.stream((double[])a).boxed().collect(Collectors.toList()));
        valueTransformer.put(o -> o.getClass().isArray() && short.class.equals(o.getClass().getComponentType()), a -> {
            short[] sa = (short[])a;
            int[] ia = new int[sa.length];
            for (int i = 0; i < sa.length; i++) {
                ia[i] = sa[i];
            }
            return Arrays.stream(ia).boxed().collect(Collectors.toList());
        });

        valueTransformer.put(o -> o.getClass().isArray() && float.class.equals(o.getClass().getComponentType()), a -> {
            float[] fa = (float[])a;
            double[] da = new double[fa.length];
            for (int i = 0; i < fa.length; i++) {
                da[i] = fa[i];
            }
            return Arrays.stream(da).boxed().collect(Collectors.toList());
        });
        valueTransformer.put(o -> o.getClass().isArray() && boolean.class.equals(o.getClass().getComponentType()), a -> {
            boolean[] ba = (boolean[])a;
            Boolean[] ra = new Boolean[ba.length];
            for (int i = 0; i < ba.length; i++) {
                ra[i] = ba[i];
            }
            return asList(ra);
        });
    }


    public AerospikeInsertQuery(String schema, String set, List<DataColumn> columns, Iterable<List<Object>> data, WritePolicy policy, boolean skipDuplicates) {
        super(schema, set, columns, data, policy, null);
        this.skipDuplicates = skipDuplicates;
        columns.stream().map(DataColumn::getName).filter("PK"::equals).findFirst().orElseThrow(() -> new IllegalArgumentException("PK is not specified"));

        indexOfPK =
                IntStream.range(0, columns.size())
                        .filter(i -> "PK".equals(columns.get(i).getName()))
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
            return new Key(schema, set, (byte[]) pk);
        }

        throw new IllegalArgumentException("Key must be either String, int, long or byte[]. " + pk.getClass() + " is not supported");
    }


    private Bin[] bins(List<Object> row) {
        Bin[] bins = new Bin[columns.size() - 1];
        for (int i = 0, j = 0; i < columns.size(); i++) {
            if (i != indexOfPK) {
                bins[j] = new Bin(columns.get(i).getName(), binValue(row.get(i)));
                j++;
            }
        }

        return bins;
    }

    private static Object binValue(Object o) {
        for(Map.Entry<Predicate<Object>, Function<Object, Object>> e : valueTransformer.entrySet()) {
            if (e.getKey().test(o)) {
                return e.getValue().apply(o);
            }
        }
        return o;
    }

    private static List<? extends Object> values(Stream<? extends Object> stream) {
        return stream.map(AerospikeInsertQuery::binValue).collect(Collectors.toList());
    }
}
