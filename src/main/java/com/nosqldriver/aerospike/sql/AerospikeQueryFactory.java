package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.PredExp;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.aerospike.sql.query.BinaryOperation;
import com.nosqldriver.aerospike.sql.query.BinaryOperation.Operator;
import com.nosqldriver.aerospike.sql.query.ColumnRefPredExp;
import com.nosqldriver.aerospike.sql.query.OperatorRefPredExp;
import com.nosqldriver.aerospike.sql.query.QueryHolder;
import com.nosqldriver.aerospike.sql.query.QueryHolder.ChainOperation;
import com.nosqldriver.aerospike.sql.query.ValueRefPredExp;
import com.nosqldriver.sql.DataColumn;
import com.nosqldriver.sql.JoinType;
import com.nosqldriver.sql.OrderItem;
import com.nosqldriver.sql.RecordExpressionEvaluator;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.expression.operators.relational.MultiExpressionList;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.UseStatement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.select.SetOperation;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.UnionOp;
import net.sf.jsqlparser.statement.update.Update;

import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.nosqldriver.sql.OrderItem.Direction.ASC;
import static com.nosqldriver.sql.OrderItem.Direction.DESC;
import static com.nosqldriver.sql.SqlLiterals.operatorKey;
import static com.nosqldriver.sql.SqlLiterals.predExpOperators;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static java.util.Optional.ofNullable;

public class AerospikeQueryFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private String schema;
    private String set;
    private final AerospikePolicyProvider policyProvider;
    private final Collection<String> indexes;
    @VisibleForPackage
    AerospikeQueryFactory(String schema, AerospikePolicyProvider policyProvider, Collection<String> indexes) {
        this.schema = schema;
        this.policyProvider = policyProvider;
        this.indexes = indexes;
    }

    @VisibleForPackage
    Function<IAerospikeClient, ResultSet> createQuery(String sql) throws SQLException {
        try {
            QueryHolder queries = new QueryHolder(schema, indexes, policyProvider);
            parserManager.parse(new StringReader(sql)).accept(new StatementVisitorAdapter() {
                @Override
                public void visit(Select select) {
                    SelectBody selectBody = select.getSelectBody();
                    createSelect(selectBody, queries);
                }


                @Override
                public void visit(Insert insert) {
                    queries.setSkipDuplicates(insert.isModifierIgnore());
                    Table table = insert.getTable();
                    if (table.getSchemaName() != null) {
                        queries.setSchema(table.getSchemaName());
                    }
                    queries.setSetName(table.getName(), ofNullable(table.getAlias()).map(Alias::getName).orElse(null));
                    set = table.getName();
                    insert.getColumns().forEach(column -> queries.addName(column.getColumnName()));

                    // Trick to support both single and multi expression list.
                    // The row data is accumulated in values. queries.addData() copies collection of values to other list
                    // After each row of multiple list the values list is cleaned. After this block queries.addData() is called
                    // again for single data list only if valules is not empty. This prevents calling queries.addData() one more time
                    // for multiple data list.
                    List<Object> values = new ArrayList<>();
                    insert.getItemsList().accept(new ItemsListVisitorAdapter() {
                        @Override
                        public void visit(MultiExpressionList multiExprList) {
                            for (ExpressionList list : multiExprList.getExprList()) {
                                values.clear();
                                visit(list);
                                queries.addData(values);
                                values.clear();
                            }
                        }

                        @Override
                        public void visit(ExpressionList expressionList) {
                            expressionList.accept(new ExpressionVisitorAdapter() {
                                @Override
                                public void visit(NullValue value) {
                                    values.add(null);
                                }

                                @Override
                                public void visit(DoubleValue value) {
                                    values.add(value.getValue());
                                }

                                @Override
                                public void visit(LongValue value) {
                                    values.add(value.getValue());
                                }


                                @Override
                                public void visit(DateValue value) {
                                    values.add(value.getValue());
                                }

                                @Override
                                public void visit(TimeValue value) {
                                    values.add(value.getValue());
                                }

                                @Override
                                public void visit(TimestampValue value) {
                                    values.add(value.getValue());
                                }

                                @Override
                                public void visit(StringValue value) {
                                    values.add(value.getValue());
                                }

                            });
                            System.out.println("visit expressions: " + expressionList);
                        }
                    });

                    if (!values.isEmpty()) {
                        queries.addData(values);
                    }
                }

                @Override
                public void visit(Update update) {
                    super.visit(update);
                }

                @Override
                public void visit(UseStatement use) {
                    schema = use.getName();
                }

                @Override
                public void visit(CreateIndex createIndex) {
                    createIndex.getIndex().getColumnsNames();
                    String columnName = createIndex.getIndex().getColumnsNames().get(0); // todo : assert if length is not  1
                    set = createIndex.getTable().getName();
                    indexes.add(createIndex.getIndex().getType() + ":" + createIndex.getIndex().getName() + ":" + columnName);
                }

                @Override
                public void visit(Drop drop) {
                    set = drop.getName().getSchemaName();
                    indexes.add(drop.getName().getName());
                }
            });

            return queries.getQuery();
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }



    private void createSelect(SelectBody selectBody, QueryHolder queries) {
        AtomicReference<Class> lastValueType = new AtomicReference<>();

        selectBody.accept(new SelectVisitorAdapter() {
            @Override
            public void visit(SetOperationList setOpList) {
                if (setOpList.getOffset() != null) {
                    queries.setOffset(setOpList.getOffset().getOffset());
                }
                if (setOpList.getLimit() != null && setOpList.getLimit().getRowCount() != null) {
                    setOpList.getLimit().getRowCount().accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(LongValue value) {
                            queries.setLimit(value.getValue());
                        }
                    });
                }
                if (setOpList.getOrderByElements() != null) {
                    setOpList.getOrderByElements().stream().map(o -> new OrderItem(o.getExpression().toString(), o.isAsc() ? ASC :DESC)).forEach(queries::addOrdering);
                }
                List<SetOperation> operations = setOpList.getOperations();
                if (operations.size() != 1) {
                    throw new IllegalArgumentException(format("Query can contain only one concatenation operation but was %d: %s", operations.size(), operations));
                }
                SetOperation operation = operations.get(0);
                final ChainOperation chainOperation;
                if (operation instanceof UnionOp) {
                    UnionOp union = (UnionOp)operation;
                    chainOperation = union.isAll() ? ChainOperation.UNION_ALL : ChainOperation.UNION;
                } else {
                    throw new UnsupportedOperationException(operation.toString());
                }


                setOpList.getSelects().forEach(sb -> createSelect(sb, queries.addSubQuery(chainOperation)));
            }

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
                                 set = tableName.getName();
                             }

                             @Override
                             public void visit(SubSelect subSelect) {
                                 createSelect(subSelect.getSelectBody(), queries.addSubQuery(ChainOperation.SUB_QUERY));
                             }
                         }
                    );
                }


                plainSelect.getSelectItems().forEach(si -> si.accept(new SelectItemVisitorAdapter() {
                    @Override
                    public void visit(SelectExpressionItem selectExpressionItem) {
                        String alias = ofNullable(selectExpressionItem.getAlias()).map(Alias::getName).orElse(null);
                        Expression expr = selectExpressionItem.getExpression();
                        Object selector = plainSelect.getDistinct() != null ? "distinct" + expr : expr; //TODO: ugly patch.
                        queries.getColumnType(selector).addColumn(expr, alias, true, queries.getSchema(), queries.getSetName());
                    }
                }));


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
                                Optional<Operator> operator = Operator.find(expr.getStringExpression());

                                if (!operator.map(Operator.EQ::equals).orElse(false)) {
                                    throw new IllegalArgumentException(format("Join condition must use = only but was %s", operator.map(Operator::operator).orElse("N/A")));
                                }
                                currentJoin.addPredExp(new OperatorRefPredExp(expr.getStringExpression()));
                            }

                            @Override
                            public void visit(Column column) {
                                System.out.println("join visit(Column column): " + column);
                                String table = column.getTable().getName();
                                String columnName = column.getColumnName();
                                if (Objects.equals(queries.getSetName(), table) || Objects.equals(queries.getSetAlias(), table)) {
                                    queries.getColumnType(column).addColumn(column, null, false, null, null);
                                    currentJoin.addPredExp(new ValueRefPredExp(table, columnName));
                                } else if (Objects.equals(currentJoin.getSetName(), table) || Objects.equals(currentJoin.getSetAlias(), table)) {
                                    currentJoin.getColumnType(column).addColumn(column, null, false, null, null);
                                    currentJoin.addPredExp(new ColumnRefPredExp(table, columnName));
                                }
                            }
                        });


                        join.getUsingColumns();
                    }
                }



                Expression where = plainSelect.getWhere();
                // Between is not supported by predicates and has to be transformed to expression like filed >= lowerValue and field <= highValue.
                // In terms of predicates additional "stringBin" and "and" predicates must be added. This is implemented using the following variables.
                AtomicBoolean between = new AtomicBoolean(false);
                AtomicInteger betweenEdge = new AtomicInteger(0);
                AtomicBoolean in = new AtomicBoolean(false);
                if (where != null) {
                    String whereExpression = where.toString();
                    //TODO: this regex does not include parentheses because they conflict with "in (1, 2, 3)", so I have to find a way to safely detect composite mathematical expressions and function calls in where clause.
                    if(Pattern.compile("[-+*/]").matcher(whereExpression).find()) {
                        queries.setWhereExpression(whereExpression);
                    }
                    AtomicBoolean predExpsEmpty = new AtomicBoolean(true);
                    BinaryOperation operation = new BinaryOperation();
                    AtomicBoolean ignoreNextOp = new AtomicBoolean(false);
                    where.accept(new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Between expr) {
                            System.out.println("visitBinaryExpression " + expr + " START");
                            between.set(true);
                            super.visit(expr);
                            Operator.BETWEEN.update(queries, operation);
                            System.out.println("visitBinaryExpression " + expr + " END");
                            queries.queries(operation.getTable()).addPredExp(predExpOperators.get(operatorKey(lastValueType.get(), "AND")).get());
                            between.set(false);
                            operation.clear();
                        }

                        public void visit(InExpression expr) {
                            in.set(true);
                            super.visit(expr);
                            expr.getRightItemsList().accept(new ItemsListVisitorAdapter() {
                                @Override
                                public void visit(ExpressionList expressionList) {
                                    Operator.IN.update(queries, operation);
                                }
                            });
                            in.set(false);
                        }


                        @Override
                        protected void visitBinaryExpression(BinaryExpression expr) {
                            super.visitBinaryExpression(expr);
                            System.out.println("visitBinaryExpression " + expr);
                            Optional<Operator> operator = Operator.find(expr.getStringExpression());
                            if (!operator.isPresent() || (operator.get().doesRequireColumn() && operation.getColumn() == null)) {
                                queries.queries(operation.getTable()).removeLastPredicates(4);
                                ignoreNextOp.set(true);
                                queries.setWhereExpression(whereExpression);
                            } else {
                                String op = expr.getStringExpression();
                                if (ignoreNextOp.get() && ("AND".equals(op) || "OR".equals(op))) {
                                    ignoreNextOp.set(false);
                                } else {
                                    operator.get().update(queries, operation);
                                    queries.queries(operation.getTable()).addPredExp(predExpOperators.get(operatorKey(lastValueType.get(), op)).get());
                                }
                            }
                            operation.clear();
                        }

                        @Override
                        public void visit(Column column) {
                            System.out.println("visit(Column column): " + column);
                            if (operation.getColumn() == null) {
                                String table = ofNullable(column.getTable()).map(Table::getName).orElse(null);
                                String name = column.getColumnName();
                                if (table == null) {
                                    // assume name is actually alias and try to retrieve real table and column name
                                    Optional<DataColumn> dc = queries.getColumnByAlias(name);
                                    if (dc.isPresent()) {
                                        table = dc.get().getTable();
                                        name = dc.get().getName();
                                    }
                                }
                                operation.setTable(table);
                                operation.setColumn(name);
                                predExpsEmpty.set(queries.queries(operation.getTable()).getPredExps().isEmpty());
                            } else {
                                operation.addValue(column.getColumnName());
                                lastValueType.set(String.class);
                                queries.queries(operation.getTable()).addPredExp(PredExp.stringBin(operation.getColumn()));
                                queries.queries(operation.getTable()).addPredExp(PredExp.stringValue(column.getColumnName()));
                            }
                        }

                        @Override
                        public void visit(LongValue value) {
                            System.out.println("visit(LongValue value): " + value);
                            operation.addValue(value.getValue());
                            if (in.get()) {
                                return;
                            }
                            queries.queries(operation.getTable()).addPredExp(PredExp.integerBin(operation.getColumn()));
                            queries.queries(operation.getTable()).addPredExp(PredExp.integerValue(value.getValue()));
                            lastValueType.set(Long.class);
                            if (between.get()) {
                                int edge = betweenEdge.incrementAndGet();
                                switch (edge) {
                                    case 1: queries.queries(operation.getTable()).addPredExp(PredExp.integerGreaterEq()); break;
                                    case 2: queries.queries(operation.getTable()).addPredExp(PredExp.integerLessEq()); break;
                                    default: throw new IllegalArgumentException("BETWEEN with more than 2 edges");
                                }
                            }
                        }

                        @Override
                        public void visit(StringValue value) {
                            System.out.println("visit(StringValue value): " + value);
                            operation.addValue(value.getValue());
                            if (in.get()) {
                                return;
                            }
                            queries.queries(operation.getTable()).addPredExp(PredExp.stringBin(operation.getColumn()));
                            queries.queries(operation.getTable()).addPredExp(PredExp.stringValue(value.getValue()));
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

                    if (!predExpsEmpty.get()) {
                        List<PredExp> predExps = queries.queries(operation.getTable()).getPredExps();
                        if (!predExps.isEmpty() && !"AndOr".equals(predExps.get(predExps.size() - 1).getClass().getSimpleName())) {
                            queries.queries(operation.getTable()).addPredExp(PredExp.and(2));
                        }
                    }
                }


                if (plainSelect.getOrderByElements() != null) {
                    plainSelect.getOrderByElements().stream().map(o -> new OrderItem(o.getExpression().toString(), o.isAsc() ? ASC :DESC)).forEach(o -> queries.addOrdering(o));
                }

                if (plainSelect.getGroupByColumnReferences() != null) {
                    plainSelect.getGroupByColumnReferences().forEach(e -> queries.addGroupField(((Column) e).getColumnName()));
                }

                if (plainSelect.getHaving() != null) {
                    queries.setHaving(plainSelect.getHaving().toString());
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
            }
        });
    }



        @VisibleForPackage
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

            WritePolicy writePolicy = new WritePolicy();

            AtomicReference<BiFunction<IAerospikeClient, Entry<Key, Record>, Boolean>> worker = new AtomicReference<>((c, e) -> false);

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
                    if (where != null) {
                        WhereVisitor whereVisitor = new WhereVisitor(AerospikeQueryFactory.this.schema, tableName.get());
                        where.accept(whereVisitor);
                        useWhereRecord.set(whereVisitor.useWhereRecord);
                        keyPredicate.set(whereVisitor.keyPredicate);
                        filterByPk.set(whereVisitor.filterByPk);
                        whereExpr.set(delete.getWhere().toString());
                    }

                    BiFunction<IAerospikeClient, Entry<Key, Record>, Boolean> deleter = (client, kr) -> client.delete(writePolicy, kr.getKey());
                    worker.set(deleter);
                }


                @Override
                public void visit(Update update) {
                    List<Table> tables = update.getTables();
                    if (tables.size() != 1) {
                        throw new IllegalArgumentException("Update statement can proceed one table only but was " + tables.size());
                    }
                    Table table = tables.get(0);
                    tableName.set(table.getName());
                    if (table.getSchemaName() != null) {
                        schema.set(table.getSchemaName());
                    }

                    List<String> columns = new ArrayList<>();
                    ExpressionVisitorAdapter columnNamesVisitor = new ExpressionVisitorAdapter() {
                        @Override
                        public void visit(Column column) {
                            columns.add(column.getColumnName());
                        }
                    };
                    update.getColumns().forEach(c -> c.accept(columnNamesVisitor));

                    List<Function<Record, Object>> valueSuppliers = new ArrayList<>();


                    for (Expression expression : update.getExpressions()) {
                        // If set statemnt uses any element of expression ()+-*/ or function call - treat it as expression.
                        // Otherwise treat it either as column reference or simple value.
                        Function<Record, Object> evaluator = new RecordExpressionEvaluator(expression.toString());
                        AtomicReference<Function<Record, Object>> extractorRef = new AtomicReference<>();

                        expression.accept(new ExpressionVisitorAdapter() {
                            // Expression related visitor
                            @Override
                            public void visit(net.sf.jsqlparser.expression.Function function) {
                                extractorRef.set(evaluator);
                            }

                            @Override
                            public void visit(Subtraction expr) {
                                extractorRef.set(evaluator);
                            }

                            @Override
                            public void visit(Addition expr) {
                                extractorRef.set(evaluator);
                            }

                            @Override
                            public void visit(Multiplication expr) {
                                extractorRef.set(evaluator);
                            }

                            @Override
                            public void visit(Division expr) {
                                extractorRef.set(evaluator);
                            }

                            @Override
                            public void visit(Parenthesis parenthesis) {
                                extractorRef.set(evaluator);
                            }

                            @Override
                            public void visit(SignedExpression expr) {
                                extractorRef.set(evaluator);
                            }

                            // Column and value visitors

                            @Override
                            public void visit(NullValue value) {
                                extractorRef.compareAndSet(null, record -> null);
                            }

                            @Override
                            public void visit(StringValue value) {
                                extractorRef.compareAndSet(null, record -> value.getValue());
                            }

                            @Override
                            public void visit(Column column) {
                                extractorRef.compareAndSet(null, record -> record.getValue(column.getColumnName()));
                            }

                            @Override
                            public void visit(DoubleValue value) {
                                extractorRef.compareAndSet(null, record -> value.getValue());
                            }

                            @Override
                            public void visit(LongValue value) {
                                extractorRef.compareAndSet(null, record -> value.getValue());
                            }
                        });

                        valueSuppliers.add(extractorRef.get());
                    }

                    Map<String, Function<Record, Object>> columnValueSuppliers =
                            IntStream.range(0, columns.size())
                                    .boxed()
                                    .collect(Collectors.toMap(columns::get, valueSuppliers::get));

                    BiFunction<IAerospikeClient, Entry<Key, Record>, Boolean> updater = (client, kr) -> {
                        client.put(writePolicy, kr.getKey(), bins(kr.getValue(), columnValueSuppliers));
                        return true;
                    };

                    worker.set(updater);


                    Expression where = update.getWhere();
                    if (where != null) {
                        WhereVisitor whereVisitor = new WhereVisitor(AerospikeQueryFactory.this.schema, tableName.get());
                        where.accept(whereVisitor);
                        useWhereRecord.set(whereVisitor.useWhereRecord);
                        keyPredicate.set(whereVisitor.keyPredicate);
                        filterByPk.set(whereVisitor.filterByPk);
                        whereExpr.set(update.getWhere().toString());
                    }

                }
            });

            if (useWhereRecord.get() && whereExpr.get() != null) {
                recordPredicate.set(new RecordExpressionEvaluator(whereExpr.get()));
            } else if (filterByPk.get()) {
                recordPredicate.set(r -> false);
            }

            return client -> {
                AtomicInteger count = new AtomicInteger(0);
                client.scanAll(policyProvider.getScanPolicy(), schema.get(), tableName.get(),
                        (key, record) -> {
                            if (keyPredicate.get().test(key) || recordPredicate.get().test(record)) {
                                worker.get().apply(client, singletonMap(key, record).entrySet().iterator().next());
                                count.incrementAndGet();
                            }
                        });
                return count.get();
            };
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }


    private Bin[] bins(Record record, Map<String, Function<Record, Object>> columnValueSuppliers) {
        return columnValueSuppliers.entrySet().stream().map(e -> new Bin(e.getKey(), e.getValue().apply(record))).toArray(Bin[]::new);
    }

//    public static String operatorKey(Class<?> type, String operand) {
//        return type.getName() + operand;
//    }


    private static class WhereVisitor extends ExpressionVisitorAdapter {
        private final String tableName;
        private final String schema;

        private Predicate<Key> keyPredicate = key -> false;
        private boolean useWhereRecord = false;
        private boolean filterByPk = false;


        private String column = null;
        private final List<Long> longParameters = new ArrayList<>();
        private final Collection<String> stringParameters = new ArrayList<>();


        private WhereVisitor(String schema, String tableName) {
            this.schema = schema;
            this.tableName = tableName;
        }

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
                if ("PK".equals(column)) {
                    Collection<Key> keys = LongStream.rangeClosed(longParameters.get(0), longParameters.get(1)).boxed().map(i -> new Key(schema, tableName, i)).collect(Collectors.toSet());
                    keyPredicate = keys::contains;

                    filterByPk = true;
                } else {
                    useWhereRecord = true;
                }
            }
        }
        public void visit(InExpression expr) {
            super.visit(expr);
            System.out.println("visit(In): " + expr);
            if ("PK".equals(column)) {
                Collection<Key> keys = longParameters.stream().map(i -> new Key(schema, tableName, i)).collect(Collectors.toSet());
                keyPredicate = keys::contains;
                filterByPk = true;
            } else {
                useWhereRecord = true;
            }
        }
        protected void visitBinaryExpression(BinaryExpression expr) {
            System.out.println("visitBinaryExpression(BinaryExpression expr): " + expr);
            super.visitBinaryExpression(expr);

            if ("PK".equals(column)) {
                Key condition = new Key(schema, tableName, longParameters.get(0));
                keyPredicate = condition::equals;
                filterByPk = true;
            } else {
                useWhereRecord = true;
            }

        }
        public void visit(Column col) {
            column = col.getColumnName();
        }
        public void visit(LongValue value) {
            longParameters.add(value.getValue());
        }
        public void visit(StringValue value) {
            stringParameters.add(value.getValue());
        }
    }


    public CCJSqlParserManager getParserManager() {
        return parserManager;
    }

    public String getSchema() {
        return schema;
    }

    public AerospikePolicyProvider getPolicyProvider() {
        return policyProvider;
    }

    public Collection<String> getIndexes() {
        return indexes;
    }

    public static Map<String, Supplier<PredExp>> getPredExpOperators() {
        return predExpOperators;
    }

    public String getSet() {
        return set;
    }
}
