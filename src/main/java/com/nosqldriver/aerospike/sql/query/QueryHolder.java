package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.Value.StringValue;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.Statement;
import com.nosqldriver.aerospike.sql.AerospikePolicyProvider;
import com.nosqldriver.sql.ExpressionAwareResultSetFactory;
import com.nosqldriver.sql.FilteredResultSet;
import com.nosqldriver.sql.OffsetLimit;
import com.nosqldriver.sql.ResultSetInvocationHandler;
import com.nosqldriver.sql.ResultSetWrapper;
import com.nosqldriver.sql.ResultSetWrapperFactory;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;

import java.sql.ResultSet;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.nosqldriver.sql.ResultSetInvocationHandler.GET_NAME;
import static com.nosqldriver.sql.ResultSetInvocationHandler.OTHER;
import static com.nosqldriver.sql.TypeTransformer.cast;
import static java.lang.String.format;
import static java.lang.String.join;

public class QueryHolder {
    private String schema;
    private final Collection<String> indexes;
    private final AerospikePolicyProvider policyProvider;
    private String set;
    private List<String> names = new ArrayList<>();
    private List<String> aliases = new ArrayList<>();
    private List<String> expressions = new ArrayList<>();
    private Collection<String> hiddenNames = new HashSet<>();
    private Collection<String> aggregatedFields = null;
    private Collection<String> groupByFields = null;

    private final Statement statement;
    private AerospikeBatchQueryBySecondaryIndex secondayIndexQuery = null;
    private AerospikeQueryByPk pkQuery = null;
    private AerospikeBatchQueryByPk pkBatchQuery = null;
    private Filter filter;
    private List<PredExp> predExps = new ArrayList<>();
    private long offset = -1;
    private long limit = -1;

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

        return wrap(createSecondaryIndexQuery());
        //return new AerospikeBatchQueryBySecondaryIndex(schema, getNames(), statement, policyProvider.getQueryPolicy());
        //throw new IllegalStateException("Query was not created"); //TODO: pass SQL here to attach it to the exception
    }

    @SafeVarargs
    private final void assertNull(Function<IAerospikeClient, ResultSet>... queries) {
        if (Arrays.stream(queries).anyMatch(Objects::nonNull)) {
            throw new IllegalStateException("More than one queires have been created");
        }
    }

    private String[] getNames(boolean hidden) {
        List<String> all = new ArrayList<>(names);
        if (hidden) {
            all.addAll(hiddenNames);
        }
        return all.toArray(new String[0]);
    }

    public void setSchema(String schema) {
        this.schema = schema;
        statement.setNamespace(schema);
    }

    public String getSchema() {
        return schema;
    }

    public void setSetName(String set) {
        this.set = set;
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
            Value[] args = names.stream().map(expr -> expr.replace('(', ':').replace(")", "")).map(StringValue::new).toArray(Value[]::new);
            statement.setAggregateFunction(getClass().getClassLoader(), "groupby.lua", "groupby", "groupby", args);
            return new AerospikeDistinctQuery(schema, getNames(false), aliases.toArray(new String[0]), statement, policyProvider.getQueryPolicy());
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
                names = new ArrayList<>(aggregatedFields);
                return new AerospikeDistinctQuery(schema, aggregatedFields.toArray(new String[0]), aliases.toArray(new String[0]), statement, policyProvider.getQueryPolicy());
            }


            statement.setAggregateFunction(getClass().getClassLoader(), "stats.lua", "stats", "single_bin_stats", fieldsForAggregation);
            return new AerospikeAggregationQuery(schema, getNames(false), aliases.toArray(new String[0]), statement, policyProvider.getQueryPolicy());
        }


        return secondayIndexQuery = new AerospikeBatchQueryBySecondaryIndex(schema, getNames(false), statement, policyProvider.getQueryPolicy());
    }

    void createPkQuery(Key key) {
        pkQuery = new AerospikeQueryByPk(schema, getNames(false), key, policyProvider.getPolicy());
    }

    void createPkBatchQuery(Key ... keys) {
        pkBatchQuery = new AerospikeBatchQueryByPk(schema, getNames(false), keys, policyProvider.getBatchPolicy());
    }

    public String getSetName() {
        return set;
    }

    public void addPredExp(PredExp predExp) {
        predExps.add(predExp);
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
        Function<IAerospikeClient, ResultSet> expressioned = client -> expressionResultSetWrappingFactory.wrap(new ResultSetWrapper(nakedQuery.apply(client), names, aliases), hiddenNames, expressions, aliases);
        Function<IAerospikeClient, ResultSet> limited = offset >= 0 || limit >= 0 ? client -> new FilteredResultSet(expressioned.apply(client), new OffsetLimit(offset < 0 ? 0 : offset, limit < 0 ? Long.MAX_VALUE : limit)) : expressioned;
        return client -> new ResultSetWrapperFactory().create(new ResultSetInvocationHandler<ResultSet>(GET_NAME | OTHER, limited.apply(client), schema, names.toArray(new String[0]), aliases.toArray(new String[0])){
            @Override
            protected <T> T get(String alias, Class<T> type) {
                if (!aliases.contains(alias) && !names.contains(alias) && !names.isEmpty()) {
                    throwAny(new SQLException(format("Column '%s' not found", alias)));
                }

                try {
                    @SuppressWarnings("unchecked")
                    T result = cast(resultSet.getObject(alias), type);
                    return result;
                } catch (SQLException e) {
                    throwAny(e);
                    return null; // just to satisfy compiler: the exception is thrown in previous line.
                }
            }

        });
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
        protected abstract String getText(Expression expr);
        public abstract void addColumn(Expression expr, String alias);
    }



    private ColumnType[] types = new ColumnType[] {
            new ColumnType((e) -> e instanceof Column) {

                @Override
                protected String getText(Expression expr) {
                    return ((Column) expr).getColumnName();
                }

                @Override
                public void addColumn(Expression expr, String alias) {
                    names.add(getText(expr));
                    expressions.add(null);
                    aliases.add(alias);
                    statement.setBinNames(getNames(true));
                }
            },

            new ColumnType(e -> e instanceof BinaryExpression || e instanceof LongValue ||  e instanceof DoubleValue || (e instanceof net.sf.jsqlparser.expression.Function && expressionResultSetWrappingFactory.getClientSideFuctionNames().contains(((net.sf.jsqlparser.expression.Function)e).getName()))) {
                @Override
                protected String getText(Expression expr) {
                    return expr.toString();
                }

                @Override
                public void addColumn(Expression expr, String alias) {
                    String column = getText(expr);
                    names.add(null);
                    expressions.add(column);
                    aliases.add(alias);
                    hiddenNames.addAll(expressionResultSetWrappingFactory.getVariableNames(column));
                    statement.setBinNames(getNames(true));
                }
            },


            new ColumnType(e -> e instanceof net.sf.jsqlparser.expression.Function) {
                @Override
                protected String getText(Expression expr) {
                    return expr.toString();
                }

                @Override
                public void addColumn(Expression expr, String alias) {
                    if (aggregatedFields == null) { // && !addition.isEmpty()) {
                        aggregatedFields = new HashSet<>();
                    }
                    List<String> addition = Optional.ofNullable(((net.sf.jsqlparser.expression.Function)expr).getParameters()).map(p -> p.getExpressions().stream().map(Object::toString).collect(Collectors.toList())).orElse(Collections.emptyList());
                    aggregatedFields.addAll(addition);
                    names.add(expr.toString());
                    aliases.add(alias);
                }
            },

            new ColumnType(e -> e instanceof String && ((String)e).startsWith("distinct")) {
                @Override
                protected String getText(Expression expr) {
                    return expr.toString();
                }

                @Override
                public void addColumn(Expression expr, String alias) {
                    if (aggregatedFields == null) {
                        aggregatedFields = new HashSet<>();
                    }
                    aggregatedFields.add("distinct" + expr.toString());
                    names.add(expr.toString());
                    aliases.add(alias);
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
}
