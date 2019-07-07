package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.Value.StringValue;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;
import com.nosqldriver.aerospike.sql.AerospikeQueryFactory;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.ExpressionAwareResultSetFactory;
import com.nosqldriver.sql.FilteredResultSet;
import com.nosqldriver.sql.JoinedResultSet;
import com.nosqldriver.sql.NameCheckResultSetWrapper;
import com.nosqldriver.sql.OffsetLimit;
import com.nosqldriver.sql.ResultSetWrapper;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.EXPRESSION;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.HIDDEN;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Optional.ofNullable;

public class QueryHolder {
    private String schema;
    private final Collection<String> indexes;
    private final AerospikePolicyProvider policyProvider;
    private String set;
    private String setAlias;
    private List<String> tables = new ArrayList<>();
    private List<String> aliases = new ArrayList<>();
    private Collection<String> hiddenNames = new HashSet<>();
    private Collection<String> aggregatedFields = null;
    private Collection<String> groupByFields = null;
    private List<DataColumn> columns = new ArrayList<>();
    private List<List<Object>> data = new ArrayList<>();
    private boolean skipDuplicates = false;

    private final Statement statement;
    private AerospikeBatchQueryBySecondaryIndex secondayIndexQuery = null;
    private AerospikeQueryByPk pkQuery = null;
    private AerospikeBatchQueryByPk pkBatchQuery = null;
    private Filter filter;
    private List<PredExp> predExps = new ArrayList<>();
    private long offset = -1;
    private long limit = -1;

    private Collection<QueryHolder> joins = new ArrayList<>();
    private boolean skipIfMissing;

    private final ExpressionAwareResultSetFactory expressionResultSetWrappingFactory = new ExpressionAwareResultSetFactory();


    public QueryHolder(String schema, Collection<String> indexes, AerospikePolicyProvider policyProvider) {
        this.schema = schema;
        this.indexes = indexes;
        this.policyProvider = policyProvider;
         statement = new Statement();
         if (schema != null) {
             statement.setNamespace(schema);
         }
    }

    public Function<IAerospikeClient, ResultSet> getQuery() {
        if (pkQuery != null) {
            assertNull(pkBatchQuery, secondayIndexQuery);
            return wrap(pkQuery);
        }
        if (pkBatchQuery != null) {
            assertNull(pkQuery, secondayIndexQuery);
            return wrap(pkBatchQuery);
        }
        if (secondayIndexQuery != null) {
            assertNull(pkQuery, pkBatchQuery);
            return wrap(secondayIndexQuery);
        }
        if (!data.isEmpty()) {
            return new AerospikeInsertQuery(schema, set, columns, data, policyProvider.getWritePolicy(), skipDuplicates);
        }

        return wrap(createSecondaryIndexQuery());
    }

    @SafeVarargs
    private final void assertNull(Function<IAerospikeClient, ResultSet>... queries) {
        if (Arrays.stream(queries).anyMatch(Objects::nonNull)) {
            throw new IllegalStateException("More than one queires have been created");
        }
    }

    private String[] getNames() {
        return columns.stream().filter(c -> c.getName() != null).map(DataColumn::getName).toArray(String[]::new);
    }

    public void setSchema(String schema) {
        this.schema = schema;
        statement.setNamespace(schema);
    }

    public String getSchema() {
        return schema;
    }

    public void setSetName(String set, String alias) {
        this.set = set;
        this.setAlias = alias;
        statement.setSetName(set);
    }

    private Function<IAerospikeClient, ResultSet>  createSecondaryIndexQuery() {
        return createSecondaryIndexQuery(filter, predExps);
    }


    private Function<IAerospikeClient, ResultSet>  createSecondaryIndexQuery(Filter filter, List<PredExp> predExps) {
        statement.setFilter(filter);
        if (predExps.size() >= 3) {
            statement.setPredExp(predExps.toArray(new PredExp[0]));
        }

        if (groupByFields != null) {
            Value[] args = Stream.concat(
                    groupByFields.stream().map(f -> "groupby:" + f),
                    columns.stream().filter(c -> DATA.equals(c.getRole())).map(DataColumn::getName).filter(expr -> expr.contains("(")).map(expr -> expr.replace('(', ':').replace(")", "")))
                    .map(StringValue::new).toArray(Value[]::new);
            statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", args);
            return new AerospikeDistinctQuery(schema, columns, statement, policyProvider.getQueryPolicy());
        }

        if (aggregatedFields != null) {
            Value[] fieldsForAggregation = aggregatedFields.stream().map(StringValue::new).toArray(Value[]::new);
            if(statement.getBinNames() != null && statement.getBinNames().length > 0) {
                throw new IllegalArgumentException("Cannot perform aggregation operation with query that contains regular fields");
            }
            Pattern p = Pattern.compile("distinct\\((\\w+)\\)");
            Optional<String> distinctExpression = aggregatedFields.stream().filter(s -> p.matcher(s).find()).findAny();
            if (distinctExpression.isPresent()) {
                if (aggregatedFields.size() > 1) {
                    throw new IllegalArgumentException("Wrong query syntax: distinct is used together with other fileds");
                }

                Matcher m = p.matcher(distinctExpression.get());
                if (!m.find()) {
                    throw new IllegalStateException(); // actually cannot happen
                }
                String groupField = m.group(1);
                statement.setAggregateFunction(getClass().getClassLoader(), "distinct.lua", "distinct", "distinct", new StringValue(groupField));
                return new AerospikeDistinctQuery(schema, columns, statement, policyProvider.getQueryPolicy());
            }


            statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "single_bin_stats", fieldsForAggregation);
            return new AerospikeAggregationQuery(schema, set, columns, statement, policyProvider.getQueryPolicy());
        }


        return secondayIndexQuery = new AerospikeBatchQueryBySecondaryIndex(schema, columns, statement, policyProvider.getQueryPolicy());
    }

    @VisibleForPackage
    void createPkQuery(Key key) {
        pkQuery = new AerospikeQueryByPk(schema, columns, key, policyProvider.getPolicy());
    }

    @VisibleForPackage
    void createPkBatchQuery(Key ... keys) {
        pkBatchQuery = new AerospikeBatchQueryByPk(schema, set, columns, keys, policyProvider.getBatchPolicy());
    }

    public String getSetName() {
        return set;
    }

    public String getSetAlias() {
        return setAlias;
    }

    public void addPredExp(PredExp predExp) {
        predExps.add(predExp);
    }


    public List<PredExp> getPredExps() {
        return predExps;
    }


    public void setFilter(Filter filter, String binName) {
        if (indexes.contains(join(".", schema, set, binName))) {
            this.filter = filter;
        }
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setLimit(long limit) {
        this.limit = limit;
    }


    private Function<IAerospikeClient, ResultSet> wrap(Function<IAerospikeClient, ResultSet> nakedQuery) {
        Function<IAerospikeClient, ResultSet> expressioned = client -> expressionResultSetWrappingFactory.wrap(new ResultSetWrapper(nakedQuery.apply(client), columns), columns);
        Function<IAerospikeClient, ResultSet> limited = offset >= 0 || limit >= 0 ? client -> new FilteredResultSet(expressioned.apply(client), new OffsetLimit(offset < 0 ? 0 : offset, limit < 0 ? Long.MAX_VALUE : limit)) : expressioned;
        Function<IAerospikeClient, ResultSet> joined = joins.isEmpty() ? limited : client -> new JoinedResultSet(limited.apply(client), joins.stream().map(join -> new JoinHolder(new JoinRetriever(client, join), new ResultSetMetadataSupplier(client, join), join.skipIfMissing)).collect(Collectors.toList()));

        return client -> new NameCheckResultSetWrapper(joined.apply(client), columns);
    }

    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwAny(Throwable e) throws E {
        throw (E)e;
    }

    public abstract class ColumnType {
        private final Predicate<Object> locator;

        protected ColumnType(Predicate<Object> locator) {
            this.locator = locator;
        }
        protected abstract String getCatalog(Expression expr);
        protected abstract String getTable(Expression expr);
        protected abstract String getText(Expression expr);
        public abstract void addColumn(Expression expr, String alias, boolean visible, String schema, String table);
    }



    private ColumnType[] types = new ColumnType[] {
            new ColumnType((e) -> e instanceof Column) {

                @Override
                protected String getCatalog(Expression expr) {
                    return ofNullable(((Column) expr).getTable()).map(Table::getSchemaName).orElse(schema);
                }

                @Override
                protected String getTable(Expression expr) {
                    return ofNullable(((Column) expr).getTable()).map(Table::getName).orElse(set);
                }

                @Override
                protected String getText(Expression expr) {
                    return ((Column) expr).getColumnName();
                }

                @Override
                public void addColumn(Expression expr, String alias, boolean visible, String catalog, String table) {
                    tables.add(getTable(expr));
                    if (!visible) {
                        hiddenNames.add(getText(expr));
                    }
                    aliases.add(alias);
                    columns.add((visible ? DATA : HIDDEN).create(getCatalog(expr), getTable(expr), getText(expr), alias));
                    statement.setBinNames(getNames());
                }
            },

            new ColumnType(e -> e instanceof BinaryExpression || e instanceof LongValue ||  e instanceof DoubleValue || (e instanceof net.sf.jsqlparser.expression.Function && expressionResultSetWrappingFactory.getClientSideFunctionNames().contains(((net.sf.jsqlparser.expression.Function)e).getName()))) {
                @Override
                protected String getCatalog(Expression expr) {
                    return schema;
                }

                @Override
                protected String getTable(Expression expr) {
                    return set;
                }

                @Override
                protected String getText(Expression expr) {
                    return expr.toString();
                }

                @Override
                public void addColumn(Expression expr, String alias, boolean visible, String catalog, String table) {
                    String column = getText(expr);
                    aliases.add(alias);
                    hiddenNames.addAll(expressionResultSetWrappingFactory.getVariableNames(column));
                    columns.add(EXPRESSION.create(getCatalog(expr), getTable(expr), getText(expr), alias));
                    expressionResultSetWrappingFactory.getVariableNames(column).forEach(v -> columns.add(HIDDEN.create(getCatalog(expr), getTable(expr), v, alias)));
                    statement.setBinNames(getNames());
                }
            },


            new ColumnType(e -> e instanceof net.sf.jsqlparser.expression.Function) {
                @Override
                protected String getCatalog(Expression expr) {
                    return schema;
                }

                @Override
                protected String getTable(Expression expr) {
                    return set;
                }

                @Override
                protected String getText(Expression expr) {
                    return expr.toString();
                }

                @Override
                public void addColumn(Expression expr, String alias, boolean visible, String catalog, String table) {
                    if (aggregatedFields == null) { // && !addition.isEmpty()) {
                        aggregatedFields = new HashSet<>();
                    }
                    List<String> addition = ofNullable(((net.sf.jsqlparser.expression.Function)expr).getParameters()).map(p -> p.getExpressions().stream().map(Object::toString).collect(Collectors.toList())).orElse(Collections.emptyList());
                    aggregatedFields.addAll(addition);
                    aliases.add(alias);
                    columns.add(DATA.create(getCatalog(expr), getTable(expr), getText(expr), alias));

                }
            },

            new ColumnType(e -> e instanceof String && ((String)e).startsWith("distinct")) {
                @Override
                protected String getCatalog(Expression expr) {
                    return ofNullable(((Column) expr).getTable()).map(Table::getSchemaName).orElse(null);
                }

                @Override
                protected String getTable(Expression expr) {
                    return ofNullable(((Column) expr).getTable()).map(Table::getName).orElse(null);
                }

                @Override
                protected String getText(Expression expr) {
                    return "distinct" + expr.toString();
                }

                @Override
                public void addColumn(Expression expr, String alias, boolean visible, String catalog, String table) {
                    if (aggregatedFields == null) {
                        aggregatedFields = new HashSet<>();
                    }
                    aggregatedFields.add("distinct" + expr.toString());
                    aliases.add(alias);
                    columns.add(DATA.create(catalog, table, getText(expr), alias));
                }
            },
    };

    public void addGroupField(String field) {
        if (groupByFields == null) {
            groupByFields = new HashSet<>();
        }
        groupByFields.add(field);
    }

    public ColumnType getColumnType(Object expr) {
        return Arrays.stream(types).filter(t -> t.locator.test(expr)).findFirst().orElseThrow(() -> new IllegalArgumentException(format("Column type %s is not supported", expr)));
    }

    public QueryHolder addJoin(boolean skipIfMissing) {
        QueryHolder join = new QueryHolder(schema, indexes, policyProvider);
        join.skipIfMissing = skipIfMissing;
        joins.add(join);
        return join;
    }

    private static class JoinRetriever implements Function<ResultSet, ResultSet> {
        private final IAerospikeClient client;
        private final QueryHolder joinQuery;

        public JoinRetriever(IAerospikeClient client, QueryHolder joinQuery) {
            this.client = client;
            this.joinQuery = joinQuery;
        }

        @Override
        public ResultSet apply(ResultSet rs) {
            QueryHolder holder = new QueryHolder(joinQuery.schema, joinQuery.indexes, joinQuery.policyProvider);
            holder.setSetName(joinQuery.getSetName(), joinQuery.setAlias);
            joinQuery.copyColumnsForTable(joinQuery.setAlias, holder);
            holder.predExps = preparePredicates(rs, joinQuery.predExps);
            return holder.getQuery().apply(client);
        }


        // TODO: try to refactor this code.
        // TODO: this method uses data from AerospikeQueryFactory. Move this data to shared place.
        private List<PredExp> preparePredicates(ResultSet rs, List<PredExp> original) {
            int n = original.size();
            List<PredExp> result = new ArrayList<>(original);
            for (int i = 0; i < n; i++) {
                PredExp exp = result.get(i);
                if (exp instanceof ValueRefPredExp) {
                    ValueRefPredExp ref = (ValueRefPredExp)exp;
                    final Object value;
                    try {
                        value = rs.getObject(ref.getName());
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }

                    if (value instanceof String) {
                        result.set(i, PredExp.stringValue((String)value));
                        if (i > 0 && result.get(i - 1) instanceof ColumnRefPredExp) {
                            result.set(i - 1, PredExp.stringBin(((ColumnRefPredExp)result.get(i - 1)).getName()));
                            if (i < n - 1 && result.get(i + 1) instanceof OperatorRefPredExp) {
                                result.set(i + 1, AerospikeQueryFactory.predExpOperators.get(AerospikeQueryFactory.operatorKey(String.class, ((OperatorRefPredExp)result.get(i + 1)).getOp())).get());
                            }
                        } else if (i < n - 1 && result.get(i + 1) instanceof ColumnRefPredExp) {
                            result.set(i + 1, PredExp.stringBin(((ColumnRefPredExp)result.get(i + 1)).getName()));
                            if (i < n - 2 && result.get(i + 2) instanceof OperatorRefPredExp) {
                                result.set(i + 2, AerospikeQueryFactory.predExpOperators.get(AerospikeQueryFactory.operatorKey(String.class, ((OperatorRefPredExp)result.get(i + 2)).getOp())).get());
                            }
                        }
                    } else if (value instanceof Number) {
                        result.set(i, PredExp.integerValue(((Number)value).longValue()));
                        if (i > 0 && result.get(i - 1) instanceof ColumnRefPredExp) {
                            result.set(i - 1, PredExp.integerBin(((ColumnRefPredExp)result.get(i - 1)).getName()));
                            if (i < n - 1 && result.get(i + 1) instanceof OperatorRefPredExp) {
                                result.set(i + 1, AerospikeQueryFactory.predExpOperators.get(AerospikeQueryFactory.operatorKey(Long.class, ((OperatorRefPredExp)result.get(i + 1)).getOp())).get());
                            }
                        } else if (i < n - 1 && result.get(i + 1) instanceof ColumnRefPredExp) {
                            result.set(i + 1, PredExp.integerBin(((ColumnRefPredExp)result.get(i + 1)).getName()));
                            if (i < n - 2 && result.get(i + 2) instanceof OperatorRefPredExp) {
                                result.set(i + 2, AerospikeQueryFactory.predExpOperators.get(AerospikeQueryFactory.operatorKey(Long.class, ((OperatorRefPredExp)result.get(i + 2)).getOp())).get());
                            }
                        }
                    } else {
                        throw new IllegalStateException(value.getClass().getName());
                    }
                }
            }
            return result;
        }

        private int index(List<PredExp> predicates, int i, Class placeholderType, int increment) {
            int j = i + increment;
            return j >= 0 && j < predicates.size() && placeholderType.equals(predicates.get(j).getClass()) ? j : -1;
        }


        private PredExp preparePredicate(ResultSet rs, PredExp definedPredExp) {
            if (!(definedPredExp instanceof ColumnRefPredExp)) {
                return definedPredExp;
            }

            ColumnRefPredExp ref = (ColumnRefPredExp)definedPredExp;
            final Object value;
            try {
                value = rs.getObject(ref.getName());
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }

            if (value instanceof String) {
                return PredExp.stringValue((String)value);
            }
            if (value instanceof Number) {
                return PredExp.integerValue(((Number)value).longValue());
            }
            throw new IllegalStateException(value.getClass().getName());
        }


    }



    public void copyColumnsForTable(String tableAlias, QueryHolder other) {
        int n = columns.size();
        for (int i = 0; i < n; i++) {
            if (tableAlias.equals(columns.get(i).getTable())) {
                other.columns.add(columns.get(i));
            }
        }
    }

    public QueryHolder queries(String table) {
        return table == null || table.equals(setAlias) ? this : joins.stream().filter(j -> table.equals(j.setAlias)).findFirst().orElseThrow(() -> new IllegalArgumentException(format("Cannot find query for table  %s", table)));
    }

    public Optional<DataColumn> getColumnByAlias(String alias) {
        return columns.stream().filter(c -> alias.equals(c.getLabel())).findFirst();
    }

    public void addName(String name)  {
        columns.add(DATA.create(schema, set, name, null));
    }

    public void addData(List<Object> dataRow)  {
        data.add(new ArrayList<>(dataRow));
    }

    public boolean isSkipDuplicates() {
        return skipDuplicates;
    }

    public void setSkipDuplicates(boolean skipDuplicates) {
        this.skipDuplicates = skipDuplicates;
    }

    private static class ResultSetMetadataSupplier implements Supplier<ResultSetMetaData> {
        private final IAerospikeClient client;
        private final QueryHolder metadataQueryHolder;
        private ResultSetMetaData metaData;

        public ResultSetMetadataSupplier(IAerospikeClient client, QueryHolder queryHolder) {
            this.client = client;
            this.metadataQueryHolder = new QueryHolder(queryHolder.schema, queryHolder.indexes, queryHolder.policyProvider);
            this.metadataQueryHolder.setSetName(queryHolder.getSetName(), queryHolder.setAlias);
            queryHolder.copyColumnsForTable(queryHolder.setAlias, this.metadataQueryHolder);
            this.metadataQueryHolder.setLimit(1);
        }

        @Override
        public ResultSetMetaData get() {
            try {
                if (metaData == null) {
                    metaData = metadataQueryHolder.getQuery().apply(client).getMetaData();
                }
                return metaData;
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
