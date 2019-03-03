package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.query.PredExp;
import com.nosqldriver.aerospike.sql.query.BinaryOperation;
import com.nosqldriver.aerospike.sql.query.QueryHolder;
import net.sf.jsqlparser.JSQLParserException;
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
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;

import java.io.StringReader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

class AerospikeQueryFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private final String schema;
    private final AerospikePolicyProvider policyProvider;
    private final Collection<String> indexes;
    private static final Map<String, Supplier<PredExp>> predExpOperators = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        predExpOperators.put(operatorKey(String.class, "="), PredExp::stringEqual);
        predExpOperators.put(operatorKey(String.class, "<>"), PredExp::stringUnequal);
        predExpOperators.put(operatorKey(String.class, "!="), PredExp::stringUnequal);
        predExpOperators.put(operatorKey(String.class, "LIKE"), () -> PredExp.stringRegex(0));

        for (Class type : new Class[] {Byte.class, Short.class, Integer.class, Long.class}) {
            predExpOperators.put(operatorKey(type, "="), PredExp::integerEqual);
            predExpOperators.put(operatorKey(type, "<>"), PredExp::integerUnequal);
            predExpOperators.put(operatorKey(type, "!="), PredExp::integerUnequal);
            predExpOperators.put(operatorKey(type, ">"), PredExp::integerGreater);
            predExpOperators.put(operatorKey(type, ">="), PredExp::integerGreaterEq);
            predExpOperators.put(operatorKey(type, "<"), PredExp::integerLess);
            predExpOperators.put(operatorKey(type, "<="), PredExp::integerLessEq);
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
                            plainSelect.getFromItem().accept(new FromItemVisitorAdapter() {
                                @Override
                                public void visit(Table tableName) {
                                    if (tableName.getSchemaName() != null) {
                                        queries.setSchema(tableName.getSchemaName());
                                    }
                                    queries.setSetName(tableName.getName());
                                }
                            });

                            plainSelect.getSelectItems().forEach(si -> si.accept(new SelectItemVisitorAdapter() {
                                @Override
                                public void visit(SelectExpressionItem selectExpressionItem) {
                                    queries.addBinName(((Column) selectExpressionItem.getExpression()).getColumnName());
                                }
                            }));

                            BinaryOperation operation = new BinaryOperation();
                            Expression where = plainSelect.getWhere();
                            if (where != null) {
                                where.accept(new ExpressionVisitorAdapter() {
                                    @Override
                                    public void visit(Between expr) {
                                        super.visit(expr);
                                        BinaryOperation.Operator.BETWEEN.update(queries, operation);
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
                                        BinaryOperation.Operator.find(expr.getStringExpression()).update(queries, operation);
                                        queries.addPredExp(predExpOperators.get(operatorKey(lastValueType.get(), expr.getStringExpression())).get());

                                    }

                                    public void visit(Column column) {
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
                                        operation.addValue(value.getValue());
                                        lastValueType.set(Long.class);
                                    }

                                    @Override
                                    public void visit(StringValue value) {
                                        operation.addValue(value.getValue());
                                        lastValueType.set(Long.class);
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
                        }
                    });
                }
            });

            return queries.getQuery();
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }

    private static String operatorKey(Class<?> type, String operand) {
        return type.getName() + operand;
    }
}
