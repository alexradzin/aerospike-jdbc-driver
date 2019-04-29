package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PredExp;
import com.nosqldriver.aerospike.sql.query.BinaryOperation;
import com.nosqldriver.aerospike.sql.query.OperatorRefPredExp;
import com.nosqldriver.aerospike.sql.query.QueryHolder;
import com.nosqldriver.aerospike.sql.query.ColumnRefPredExp;
import com.nosqldriver.aerospike.sql.query.ValueRefPredExp;
import com.nosqldriver.sql.JoinType;
import com.nosqldriver.sql.RecordPredicate;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;

import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static java.lang.String.format;
import static java.util.Optional.ofNullable;

public class AerospikeQueryFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private final String schema;
    private final AerospikePolicyProvider policyProvider;
    private final Collection<String> indexes;
    public static final Map<String, Supplier<PredExp>> predExpOperators = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        predExpOperators.put(operatorKey(String.class, "="), PredExp::stringEqual);
        predExpOperators.put(operatorKey(String.class, "<>"), PredExp::stringUnequal);
        predExpOperators.put(operatorKey(String.class, "!="), PredExp::stringUnequal);
        predExpOperators.put(operatorKey(String.class, "LIKE"), () -> PredExp.stringRegex(0));
        predExpOperators.put(operatorKey(String.class, "AND"), () -> PredExp.and(2));
        predExpOperators.put(operatorKey(String.class, "OR"), () -> PredExp.or(2));

        for (Class type : new Class[] {Byte.class, Short.class, Integer.class, Long.class}) {
            predExpOperators.put(operatorKey(type, "="), PredExp::integerEqual);
            predExpOperators.put(operatorKey(type, "<>"), PredExp::integerUnequal);
            predExpOperators.put(operatorKey(type, "!="), PredExp::integerUnequal);
            predExpOperators.put(operatorKey(type, ">"), PredExp::integerGreater);
            predExpOperators.put(operatorKey(type, ">="), PredExp::integerGreaterEq);
            predExpOperators.put(operatorKey(type, "<"), PredExp::integerLess);
            predExpOperators.put(operatorKey(type, "<="), PredExp::integerLessEq);
            predExpOperators.put(operatorKey(type, "AND"), () -> PredExp.and(2));
            predExpOperators.put(operatorKey(type, "OR"), () -> PredExp.or(2));
        }

    }


    AerospikeQueryFactory(String schema, AerospikePolicyProvider policyProvider, Collection<String> indexes) {
        this.schema = schema;
        this.policyProvider = policyProvider;
        this.indexes = indexes;
    }

    Function<IAerospikeClient, ResultSet> createQuery(String sql) throws SQLException {
        try {
            QueryHolder queries = new QueryHolder(schema, indexes, policyProvider);
            AtomicReference<Class> lastValueType = new AtomicReference<>();
            parserManager.parse(new StringReader(sql)).accept(new StatementVisitorAdapter() {
                @Override
                public void visit(Select select) {
                    select.getSelectBody().accept(new SelectVisitorAdapter() {
                        @Override
                        public void visit(PlainSelect plainSelect) {
                            if (plainSelect.getFromItem() != null) {
                                plainSelect.getFromItem().accept(new FromItemVisitorAdapter() {
                                    @Override
                                    public void visit(Table tableName) {
                                        if (tableName.getSchemaName() != null) {
                                            queries.setSchema(tableName.getSchemaName());
                                        }
                                        queries.setSetName(tableName.getName(), ofNullable(tableName.getAlias()).map(Alias::getName).orElse(null));
                                    }
                                });
                            }



                            plainSelect.getSelectItems().forEach(si -> si.accept(new SelectItemVisitorAdapter() {
                                @Override
                                public void visit(SelectExpressionItem selectExpressionItem) {
                                    String alias = ofNullable(selectExpressionItem.getAlias()).map(Alias::getName).orElse(null);
                                    Expression expr = selectExpressionItem.getExpression();
                                    Object selector = plainSelect.getDistinct() != null ? "distinct" + expr : expr; //TODO: ugly patch.
                                    queries.getColumnType(selector).addColumn(expr, alias);
                                }
                            }));

                            Expression where = plainSelect.getWhere();
                            // Between is not supported by predicates and has to be transformed to expression like filed >= lowerValue and field <= highValue.
                            // In terms of predicates additional "stringBin" and "and" predicates must be added. This is implemented using the following variables.
                            AtomicBoolean between = new AtomicBoolean(false);
                            AtomicInteger betweenEdge = new AtomicInteger(0);
                            if (where != null) {
                                where.accept(new ExpressionVisitorAdapter() {
                                    BinaryOperation operation = new BinaryOperation();
                                    @Override
                                    public void visit(Between expr) {
                                        System.out.println("visitBinaryExpression " + expr + " START");
                                        between.set(true);
                                        super.visit(expr);
                                        BinaryOperation.Operator.BETWEEN.update(queries, operation);
                                        System.out.println("visitBinaryExpression " + expr + " END");
                                        queries.addPredExp(predExpOperators.get(operatorKey(lastValueType.get(), "AND")).get());
                                        between.set(false);
                                        operation.clear();
                                    }

                                    public void visit(InExpression expr) {
                                        super.visit(expr);
                                        expr.getRightItemsList().accept(new ItemsListVisitorAdapter() {
                                            @Override
                                            public void visit(ExpressionList expressionList) {
                                                BinaryOperation.Operator.IN.update(queries, operation);
                                            }
                                        });
                                    }


                                    @Override
                                    protected void visitBinaryExpression(BinaryExpression expr) {
                                        super.visitBinaryExpression(expr);
                                        System.out.println("visitBinaryExpression " + expr);
                                        BinaryOperation.Operator.find(expr.getStringExpression()).update(queries, operation);
                                        queries.addPredExp(predExpOperators.get(operatorKey(lastValueType.get(), expr.getStringExpression())).get());
                                        operation.clear();
                                    }

                                    @Override
                                    public void visit(Column column) {
                                        System.out.println("visit(Column column): " + column);
                                        if (operation.getColumn() == null) {
                                            operation.setColumn(column.getColumnName());
                                        } else {
                                            operation.addValue(column.getColumnName());
                                            lastValueType.set(String.class);
                                            queries.addPredExp(PredExp.stringBin(operation.getColumn()));
                                            queries.addPredExp(PredExp.stringValue(column.getColumnName()));
                                        }
                                    }

                                    @Override
                                    public void visit(LongValue value) {
                                        System.out.println("visit(LongValue value): " + value);
                                        operation.addValue(value.getValue());
                                        queries.addPredExp(PredExp.integerBin(operation.getColumn()));
                                        queries.addPredExp(PredExp.integerValue(value.getValue()));
                                        lastValueType.set(Long.class);
                                        if (between.get()) {
                                            int edge = betweenEdge.incrementAndGet();
                                            switch (edge) {
                                                case 1: queries.addPredExp(PredExp.integerGreaterEq()); break;
                                                case 2: queries.addPredExp(PredExp.integerLessEq()); break;
                                                default: throw new IllegalArgumentException("BETWEEN with more than 2 edges");
                                            }
                                        }
                                    }

                                    @Override
                                    public void visit(StringValue value) {
                                        System.out.println("visit(StringValue value): " + value);
                                        operation.addValue(value.getValue());
                                        queries.addPredExp(PredExp.stringBin(operation.getColumn()));
                                        queries.addPredExp(PredExp.stringValue(value.getValue()));
                                        lastValueType.set(String.class);
                                    }

                                    @Override
                                    public void visit(DateValue value) {
                                        visit(new LongValue(value.getValue().getTime()));
                                    }

                                    @Override
                                    public void visit(TimeValue value) {
                                        visit(new LongValue(value.getValue().getTime()));
                                    }

                                    @Override
                                    public void visit(TimestampValue value) {
                                        visit(new LongValue(value.getValue().getTime()));
                                    }
                                });
                            }


                            if (plainSelect.getGroupByColumnReferences() != null) {
                                plainSelect.getGroupByColumnReferences().forEach(e -> queries.addGroupField(((Column) e).getColumnName()));
                            }

                            if (plainSelect.getOffset() != null) {
                                queries.setOffset(plainSelect.getOffset().getOffset());
                            }

                            if (plainSelect.getLimit() != null && plainSelect.getLimit().getRowCount() != null) {
                                plainSelect.getLimit().getRowCount().accept(new ExpressionVisitorAdapter() {
                                    @Override
                                    public void visit(LongValue value) {
                                        queries.setLimit(value.getValue());
                                    }
                                });
                            }


                            if (plainSelect.getJoins() != null) {
                                for (Join join : plainSelect.getJoins()) {
                                    QueryHolder currentJoin = queries.addJoin(JoinType.skipIfMissing(JoinType.getTypes(join)));

                                    if (join.getRightItem() != null) {
                                        join.getRightItem().accept(new FromItemVisitorAdapter() {
                                            @Override
                                            public void visit(Table tableName) {
                                                if (tableName.getSchemaName() != null) {
                                                    currentJoin.setSchema(tableName.getSchemaName());
                                                }
                                                currentJoin.setSetName(tableName.getName(), ofNullable(tableName.getAlias()).map(Alias::getName).orElse(null));
                                                String joinedSetAlias = currentJoin.getSetAlias();
                                                if (joinedSetAlias != null) {
                                                    queries.copyColumnsForTable(joinedSetAlias, currentJoin);
                                                }
                                            }
                                        });
                                    }

                                    join.getOnExpression().accept(new ExpressionVisitorAdapter() {
                                        @Override
                                        protected void visitBinaryExpression(BinaryExpression expr) {
                                            super.visitBinaryExpression(expr);
                                            System.out.println("join visitBinaryExpression " + expr);
                                            BinaryOperation.Operator operator = BinaryOperation.Operator.find(expr.getStringExpression());
                                            if (!BinaryOperation.Operator.EQ.equals(operator)) {
                                                throw new IllegalArgumentException(format("Join condition must use = only but was %s", operator.operator()));
                                            }
                                            currentJoin.addPredExp(new OperatorRefPredExp(expr.getStringExpression()));
                                        }

                                        @Override
                                        public void visit(Column column) {
                                            System.out.println("join visit(Column column): " + column);
                                            String table = column.getTable().getName();
                                            String columnName = column.getColumnName();
                                            if (Objects.equals(queries.getSetName(), table) || Objects.equals(queries.getSetAlias(), table)) {
                                                queries.getColumnType(column).addColumn(column, null, false);
                                                currentJoin.addPredExp(new ValueRefPredExp(table, columnName));
                                            } else if (Objects.equals(currentJoin.getSetName(), table) || Objects.equals(currentJoin.getSetAlias(), table)) {
                                                currentJoin.getColumnType(column).addColumn(column, null, false);
                                                currentJoin.addPredExp(new ColumnRefPredExp(table, columnName));
                                            }
                                        }
                                    });


                                    join.getUsingColumns();
                                }
                            }



                        }
                    });
                }
            });

            return queries.getQuery();
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }


    Function<IAerospikeClient, Integer> createUpdate(String sql) throws SQLException {
        try {

            AtomicInteger limit = new AtomicInteger(0);
            AtomicReference<String> tableName = new AtomicReference<>(null);
            AtomicReference<String> schema = new AtomicReference<>(AerospikeQueryFactory.this.schema);
            AtomicReference<String> whereExpr = new AtomicReference<>(null);

            AtomicReference<Predicate<Key>> keyPredicate = new AtomicReference<>(key -> false);
            AtomicReference<Predicate<Record>> recordPredicate = new AtomicReference<>(key -> true);
            AtomicBoolean useWhereRecord = new AtomicBoolean(false);
            AtomicBoolean filterByPk = new AtomicBoolean(false);

            parserManager.parse(new StringReader(sql)).accept(new StatementVisitorAdapter() {
                @Override
                public void visit(Delete delete) {
                    Table table = delete.getTable();
                    if (table != null) {
                        tableName.set(table.getName());
                        if (table.getSchemaName() != null) {
                            schema.set(table.getSchemaName());
                        }
                    }

                    if (delete.getLimit() != null && delete.getLimit().getRowCount() != null) {
                        delete.getLimit().getRowCount().accept(new ExpressionVisitorAdapter() {
                            @Override
                            public void visit(LongValue value) {
                                limit.set((int)value.getValue());
                            }
                        });
                    }
                    Expression where = delete.getWhere();
                    AtomicReference<String> column = new AtomicReference<>(null);
                    List<Long> longParameters = new ArrayList<>();
                    Collection<String> stringParameters = new ArrayList<>();

                    if (where != null) {
                        where.accept(new ExpressionVisitorAdapter() {
                            public void visit(Between expr) {
                                System.out.println("visit(Between): " + expr);
                                super.visit(expr);
                                if (!stringParameters.isEmpty()) {
                                    throw new IllegalArgumentException("BETWEEN cannot be applied to string");
                                }

                                if (!longParameters.isEmpty()) {
                                    if (longParameters.size() != 2) {
                                        throw new IllegalArgumentException("BETWEEN must have exactly 2 edges");
                                    }
                                    if ("PK".equals(column.get())) {
                                        Collection<Key> keys = LongStream.rangeClosed(longParameters.get(0), longParameters.get(1)).boxed().map(i -> new Key(schema.get(), tableName.get(), i)).collect(Collectors.toSet());
                                        keyPredicate.set(keys::contains);

                                        filterByPk.set(true);
                                    } else {
                                        useWhereRecord.set(true);
                                    }
                                }
                            }
                            public void visit(InExpression expr) {
                                super.visit(expr);
                                System.out.println("visit(In): " + expr);
                                if ("PK".equals(column.get())) {
                                    Collection<Key> keys = longParameters.stream().map(i -> new Key(schema.get(), tableName.get(), i)).collect(Collectors.toSet());
                                    keyPredicate.set(keys::contains);
                                    filterByPk.set(true);
                                } else {
                                    useWhereRecord.set(true);
                                }
                            }
                            protected void visitBinaryExpression(BinaryExpression expr) {
                                System.out.println("visitBinaryExpression(BinaryExpression expr): " + expr);
                                super.visitBinaryExpression(expr);

                                if ("PK".equals(column.get())) {
                                    Key condition = new Key(schema.get(), tableName.get(), longParameters.get(0));
                                    keyPredicate.set(condition::equals);
                                    filterByPk.set(true);
                                } else {
                                    useWhereRecord.set(true);
                                }

                            }
                            public void visit(Column col) {
                                column.set(col.getColumnName());
                                System.out.println("visit(Column column): " + column);
                            }
                            public void visit(LongValue value) {
                                System.out.println("visit(LongValue): " + value);
                                longParameters.add(value.getValue());
                            }
                            public void visit(StringValue value) {
                                System.out.println("visit(StringValue): " + value);
                                stringParameters.add(value.getValue());
                            }
                        });
                        whereExpr.set(delete.getWhere().toString());
                    }
                }
            });

            if (useWhereRecord.get() && whereExpr.get() != null) {
                recordPredicate.set(new RecordPredicate(whereExpr.get()));
            } else if (filterByPk.get()) {
                recordPredicate.set(r -> false);
            }

            return client -> {
                AtomicInteger count = new AtomicInteger(0);
                client.scanAll(policyProvider.getScanPolicy(), schema.get(), tableName.get(),
                        (key, record) -> {
                            if (keyPredicate.get().test(key) || recordPredicate.get().test(record)) {
                                client.delete(new WritePolicy(), key);
                                count.incrementAndGet();
                            }
                        });
                return count.get();
            };
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }


    public static String operatorKey(Class<?> type, String operand) {
        return type.getName() + operand;
    }
}
