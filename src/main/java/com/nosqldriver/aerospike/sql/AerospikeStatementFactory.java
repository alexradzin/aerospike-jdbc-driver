package com.nosqldriver.aerospike.sql;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

class AerospikeStatementFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private final String schema;

    AerospikeStatementFactory(String schema) {
        this.schema = schema;
    }


    private static class BinaryOperation {
        private String column;
        private List<Object> values = new ArrayList<>(2);

        public BinaryOperation() {
        }

        public BinaryOperation(String column, List<Object> values) {
            this.column = column;
            this.values = values;
        }

        enum Operator {
            EQ("=") {
                @Override
                public Statement update(Statement statement, BinaryOperation operation) {
                    Object value = operation.values.get(0);
                    final Filter filter;
                    if (value instanceof Number) {
                        filter = Filter.equal(operation.column, ((Number)value).longValue());
                    } else if (value instanceof String) {
                        filter = Filter.equal(operation.column, (String)value);
                    } else {
                        throw new IllegalArgumentException(String.format("Filter by %s is not supported right now. Use either number or string", value == null ? null : value.getClass()));
                    }

                    statement.setFilter(filter);
                    return statement;
                }
            },
            GT(">") {
                @Override
                public Statement update(Statement statement, BinaryOperation operation) {
                    List<Object> values = Arrays.asList(operation.values.get(0), Long.MAX_VALUE);
                    return BETWEEN.update(statement, new BinaryOperation(operation.column, values));
                }
            },
            LT("<") {
                @Override
                public Statement update(Statement statement, BinaryOperation operation) {
                    List<Object> values = Arrays.asList(Long.MIN_VALUE, operation.values.get(0));
                    return BETWEEN.update(statement, new BinaryOperation(operation.column, values));
                }
            },
            BETWEEN("BETWEEN") {
                @Override
                public Statement update(Statement statement, BinaryOperation operation) {
                    statement.setFilter(Filter.range(operation.column, ((Number)operation.values.get(0)).longValue(), ((Number)operation.values.get(1)).longValue()));
                    return statement;
                }
            },
            ;

            private static Map<String, Operator> operators = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            static {
                operators.putAll(Arrays.stream(Operator.values()).collect(Collectors.toMap(e -> e.operator, e -> e)));
            }
            private final String operator;

            Operator(String operator) {
                this.operator = operator;
            }

            public abstract Statement update(Statement statement, BinaryOperation operation);
            public static Operator find(String op) {
                return Optional.ofNullable(operators.get(op)).orElseThrow(() -> new IllegalArgumentException(op));
            }

        }

        public void setColumn(String column) {
            this.column = column;
        }

        public void addValue(Object value) {
            values.add(value);
        }
    }


    Statement createStatement(String sql) throws SQLException {
        try {
            Statement statement = new Statement();
            Collection<String> selectedColumns = new ArrayList<>();
            parserManager.parse(new StringReader(sql)).accept(new StatementVisitorAdapter() {
                @Override
                public void visit(Select select) {
                    select.getSelectBody().accept(new SelectVisitorAdapter() {
                        @Override
                        public void visit(PlainSelect plainSelect) {
                            plainSelect.getFromItem().accept(new FromItemVisitorAdapter() {
                                @Override
                                public void visit(Table tableName) {
                                    statement.setNamespace(tableName.getSchemaName());
                                    statement.setSetName(tableName.getName());
                                }
                            });

                            plainSelect.getSelectItems().forEach(si -> si.accept(new SelectItemVisitorAdapter() {
                                @Override
                                public void visit(SelectExpressionItem selectExpressionItem) {
                                    selectedColumns.add(((Column) selectExpressionItem.getExpression()).getColumnName());
                                }
                            }));

                            BinaryOperation operation = new BinaryOperation();
                            plainSelect.getWhere().accept(new ExpressionVisitorAdapter() {
                                @Override
                                public void visit(Between expr) {
                                    super.visit(expr);
                                    BinaryOperation.Operator.BETWEEN.update(statement, operation);
                                }


                                @Override
                                protected void visitBinaryExpression(BinaryExpression expr) {
                                    super.visitBinaryExpression(expr);
                                    BinaryOperation.Operator.find(expr.getStringExpression()).update(statement, operation);
                                }

                                public void visit(Column column) {
                                    System.out.println( column.getColumnName() );
                                    operation.setColumn(column.getColumnName());
                                }
                                @Override
                                public void visit(LongValue value) {
                                    System.out.println( value.getValue() );
                                    operation.addValue(value.getValue());
                                }

                                @Override
                                public void visit(StringValue value) {
                                    System.out.println( value.getValue() );
                                    operation.addValue(value.getValue());
                                }
                            });

                        }
                    });
                }
            });

            statement.setBinNames(selectedColumns.toArray(new String[0]));
            return statement;
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }
}
