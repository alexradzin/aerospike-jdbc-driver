package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

class AerospikeQueryFactory {
    private CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private final String schema;

    AerospikeQueryFactory(String schema) {
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
                public Queries update(Queries queries, AerospikeQueryFactory.BinaryOperation operation) {
                    Object value = operation.values.get(0);
                    if ("PK".equals(operation.column)) {
                        final Key key = createKey(value, queries);
                        queries.pkQuery = new AerospikeQueryByPk(queries.getSchema(), queries.names == null ? null : queries.names.toArray(new String[0]), key);
                    } else {
                        final Filter filter = createFilter(value, operation.column);
                        queries.statement.setFilter(filter);
                        queries.secondayIndexQuery = new AerospikeBatchQueryBySecondaryIndex(queries.getSchema(), queries.getNames(), queries.statement);
                    }
                    return queries;
                }

                private Key createKey(Object value, Queries queries) {
                    final Key key;
                    final String schema = queries.getSchema();
                    if (value instanceof Long) {
                        key = new Key(schema, queries.set, (Long)value);
                    } else if (value instanceof Integer) {
                        key = new Key(schema, queries.set, (Integer)value);
                    } else if (value instanceof Number) {
                        key = new Key(schema, queries.set, ((Number)value).intValue());
                    } else if (value instanceof String) {
                        key = new Key(schema, queries.set, (String)value);
                    } else {
                        throw new IllegalArgumentException(format("Filter by %s is not supported right now. Use either number or string", value == null ? null : value.getClass()));
                    }
                    return key;
                }

                private Filter createFilter(Object value, String column) {
                    final Filter filter;
                    if (value instanceof Number) {
                        filter = Filter.equal(column, ((Number) value).longValue());
                    } else if (value instanceof String) {
                        filter = Filter.equal(column, (String) value);
                    } else {
                        throw new IllegalArgumentException(format("Filter by %s is not supported right now. Use either number or string", value == null ? null : value.getClass()));
                    }
                    return filter;
                }
            },
            GT(">") {
                @Override
                public Queries update(Queries queries, AerospikeQueryFactory.BinaryOperation operation) {
                    List<Object> values = Arrays.asList(operation.values.get(0), Long.MAX_VALUE);
                    return BETWEEN.update(queries, new AerospikeQueryFactory.BinaryOperation(operation.column, values));
                }
            },
            LT("<") {
                @Override
                public Queries update(Queries queries, AerospikeQueryFactory.BinaryOperation operation) {
                    List<Object> values = Arrays.asList(Long.MIN_VALUE, operation.values.get(0));
                    return BETWEEN.update(queries, new AerospikeQueryFactory.BinaryOperation(operation.column, values));
                }
            },
            BETWEEN("BETWEEN") {
                @Override
                public Queries update(Queries queries, AerospikeQueryFactory.BinaryOperation operation) {
                    queries.statement.setFilter(Filter.range(operation.column, ((Number)operation.values.get(0)).longValue(), ((Number)operation.values.get(1)).longValue()));
                    queries.secondayIndexQuery = new AerospikeBatchQueryBySecondaryIndex(queries.getSchema(), queries.getNames(), queries.statement);
                    return queries;
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

            public abstract Queries update(Queries queries, AerospikeQueryFactory.BinaryOperation operation);
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

    private static class Queries {
        private String schema;
        private String set;
        private List<String> names = new ArrayList<>();

        final Statement statement;
        AerospikeBatchQueryBySecondaryIndex secondayIndexQuery = null;
        AerospikeQueryByPk pkQuery = null;
        AerospikeBatchQueryByPk pkBatchQuery = null;


        private Queries(String schema) {
            this.schema = schema;
             statement = new Statement();
             if (schema != null) {
                 statement.setNamespace(schema);
             }
        }

        Function<IAerospikeClient, java.sql.ResultSet> getQuery() {
            if (pkQuery != null) {
                assertNull(pkBatchQuery, secondayIndexQuery);
                return pkQuery;
            }
            if (pkBatchQuery != null) {
                assertNull(pkQuery, secondayIndexQuery);
                return pkBatchQuery;
            }
            if (secondayIndexQuery != null) {
                assertNull(pkQuery, pkBatchQuery);
                return secondayIndexQuery;
            }
            return new AerospikeBatchQueryBySecondaryIndex(schema, getNames(), statement);
            //throw new IllegalStateException("Query was not created"); //TODO: pass SQL here to attach it to the exception
        }

        @SafeVarargs
        private final void assertNull(Function<IAerospikeClient, java.sql.ResultSet>... queries) {
            if (Arrays.stream(queries).anyMatch(Objects::nonNull)) {
                throw new IllegalStateException("More than one queires have been created");
            }
        }

        private String[] getNames() {
            return names.toArray(new String[0]);
        }

        private void setSchema(String schema) {
            this.schema = schema;
            statement.setNamespace(schema);
        }

        private String getSchema() {
            return schema;
        }

        private void setSetName(String set) {
            this.set = set;
            statement.setSetName(set);
        }

        private void addBinName(String name) {
            names.add(name);
            statement.setBinNames(getNames());
        }
    }

    Function<IAerospikeClient, ResultSet> createQuery(String sql) throws SQLException {
        try {
            //Statement statement = new Statement();
            //statement.setNamespace(schema);
            Collection<String> selectedColumns = new ArrayList<>();
            Queries queries = new Queries(schema);
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
                                        //queries.statement.setNamespace(tableName.getSchemaName());
                                        queries.setSchema(tableName.getSchemaName());
                                    }
                                    //queries.statement.setSetName(tableName.getName());
                                    //queries.set = tableName.getName();
                                    queries.setSetName(tableName.getName());
                                }
                            });

                            plainSelect.getSelectItems().forEach(si -> si.accept(new SelectItemVisitorAdapter() {
                                @Override
                                public void visit(SelectExpressionItem selectExpressionItem) {
                                    selectedColumns.add(((Column) selectExpressionItem.getExpression()).getColumnName());
                                    //queries.names.add(((Column) selectExpressionItem.getExpression()).getColumnName());
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
                                                System.out.println("llllllllllllllllllllllllllll" + expressionList);
                                            }
                                        });
                                    }


                                    @Override
                                    protected void visitBinaryExpression(BinaryExpression expr) {
                                        super.visitBinaryExpression(expr);
                                        BinaryOperation.Operator.find(expr.getStringExpression()).update(queries, operation);
                                    }

                                    public void visit(Column column) {
                                        System.out.println(column.getColumnName());
                                        operation.setColumn(column.getColumnName());
                                    }

                                    @Override
                                    public void visit(LongValue value) {
                                        System.out.println(value.getValue());
                                        operation.addValue(value.getValue());
                                    }

                                    @Override
                                    public void visit(StringValue value) {
                                        System.out.println(value.getValue());
                                        operation.addValue(value.getValue());
                                    }
                                });
                            }
                        }
                    });
                }
            });

            //queries.statement.setBinNames(selectedColumns.toArray(new String[0]));
            //queries.names = new ArrayList<>(selectedColumns);
            return queries.getQuery();
            //return new AerospikeBatchQueryBySecondaryIndex(statement.getNamespace(), selectedColumns.toArray(new String[0]), statement);
        } catch (JSQLParserException e) {
            throw new SQLException(e);
        }
    }
}
