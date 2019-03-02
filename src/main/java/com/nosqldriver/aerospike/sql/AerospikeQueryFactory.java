package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.nosqldriver.aerospike.sql.query.BinaryOperation;
import com.nosqldriver.aerospike.sql.query.QueryHolder;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
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
import java.util.function.Function;

class AerospikeQueryFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private final String schema;
    private final AerospikePolicyProvider policyProvider;

    AerospikeQueryFactory(String schema, AerospikePolicyProvider policyProvider) {
        this.schema = schema;
        this.policyProvider = policyProvider;
    }

    Function<IAerospikeClient, ResultSet> createQuery(String sql) throws SQLException {
        try {
            QueryHolder queries = new QueryHolder(schema, policyProvider);
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
                                    }

                                    public void visit(Column column) {
                                        if (operation.getColumn() == null) {
                                            operation.setColumn(column.getColumnName());
                                        } else {
                                            operation.addValue(column.getColumnName());
                                        }
                                    }

                                    @Override
                                    public void visit(LongValue value) {
                                        operation.addValue(value.getValue());
                                    }

                                    @Override
                                    public void visit(StringValue value) {
                                        operation.addValue(value.getValue());
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
}
