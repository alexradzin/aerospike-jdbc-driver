package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.ResultSet;
import com.nosqldriver.sql.BaseSchemalessResultSet;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.GenericTypeDiscoverer;
import com.nosqldriver.sql.TypeDiscoverer;
import com.nosqldriver.util.CustomDeserializerManager;
import com.nosqldriver.util.ValueExtractor;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;


public class ResultSetOverAerospikeResultSet extends BaseSchemalessResultSet<Map<String, Object>> {
    protected final ResultSet rs;
    private static final Function<Record, Map<String, Object>> recordDataExtractor = record -> record != null ? record.bins : emptyMap();
    private static final Function<KeyRecord, Map<String, Object>> keyRecordDataExtractor = keyRecord -> keyRecord != null ? recordDataExtractor.apply(keyRecord.record) : emptyMap();
    private static final Pattern functionOfField = Pattern.compile("\\w+\\(\\s*(\\w+)\\s*\\)");
    private final ValueExtractor valueExtractor = new ValueExtractor();


    public ResultSetOverAerospikeResultSet(Statement statement, String schema, String table, List<DataColumn> columns, ResultSet rs, TypeDiscoverer typeDiscoverer) {
        super(statement, schema, table, columns, typeDiscoverer);
        this.rs = rs;
    }


    public ResultSetOverAerospikeResultSet(Statement statement, String schema, String table, List<DataColumn> columns, ResultSet rs, BiFunction<String, String, Iterable<KeyRecord>> keyRecordsFetcher, CustomDeserializerManager cdm) {
        super(statement, schema, table, columns,
                columns1 -> {
                            Collection<DataColumn> referencedFields = new HashSet<>();
                            boolean shouldDiscoverData = false;
                            for (DataColumn c : columns1) {
                                String name = c.getName();
                                if (name.startsWith("count(")) {
                                    c.withType(Types.BIGINT);
                                } else if (name.startsWith("avg(") || name.startsWith("sumsqs(")) {
                                    c.withType(Types.DOUBLE);
                                } else if (name.contains("(")) {
                                    Matcher m = functionOfField.matcher(name);
                                    if (m.find()) {
                                        referencedFields.add(c);
                                    }
                                    shouldDiscoverData = true;
                                }
                            }

                            if (shouldDiscoverData) {
                                Stream<DataColumn> regularColumns = columns.stream().filter(c -> c.getType() == 0).filter(c -> !c.getName().contains("("));
                                Collection<DataColumn> specialFunctions = referencedFields.stream().map(e -> {
                                    Matcher m = functionOfField.matcher(e.getName());
                                    return m.find() ? DataColumn.DataColumnRole.HIDDEN.create(e.getCatalog(), e.getTable(), m.group(1), null) : e;
                                }).collect(toList());
                                Collection<DataColumn> uniqueSpecialColumns = new TreeSet<>(Comparator.comparing(DataColumn::getName));
                                uniqueSpecialColumns.addAll(specialFunctions);
                                new GenericTypeDiscoverer<>(keyRecordsFetcher, keyRecordDataExtractor, cdm).discoverType(Stream.concat(regularColumns, specialFunctions.stream()).collect(toList()));
                                Map<String, DataColumn> name2SpecialColumn = uniqueSpecialColumns.stream().collect(toMap(DataColumn::getName, c -> c));

                                for (DataColumn c : referencedFields) {
                                    Matcher m = functionOfField.matcher(c.getName());
                                    if (m.find()) {
                                        String name = m.group(1);
                                        c.withType(name2SpecialColumn.get(name).getType());
                                    }
                                }
                            }
                            return columns1;
                });
        this.rs = rs;
    }



    @Override
    public void close() throws SQLException {
        rs.close();
        super.close();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Map<String, Object> getRecord() {
        return (Map<String, Object>)rs.getObject();
    }

    @Override
    protected Object getValue(Map<String, Object> record, String label) {
        return valueExtractor.getValue(record, label);
    }

    @Override
    protected String getString(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), String.class);
    }

    @Override
    protected boolean getBoolean(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), Boolean.class);
    }

    @Override
    protected byte getByte(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), byte.class);
    }

    @Override
    protected short getShort(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), short.class);
    }

    @Override
    protected int getInt(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), int.class);
    }

    @Override
    protected long getLong(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), long.class);
    }

    @Override
    protected float getFloat(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), float.class);
    }

    @Override
    protected double getDouble(Map<String, Object> record, String label) throws SQLException {
        return cast(getValue(record, label), double.class);
    }

    protected boolean moveToNext() {
        return rs.next();
    }
}
