package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PredExp;
import com.nosqldriver.VisibleForPackage;
import com.nosqldriver.util.SneakyThrower;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;

import static com.aerospike.client.query.PredExp.integerBin;
import static com.aerospike.client.query.PredExp.integerEqual;
import static com.aerospike.client.query.PredExp.integerValue;
import static com.aerospike.client.query.PredExp.or;
import static com.aerospike.client.query.PredExp.stringBin;
import static com.aerospike.client.query.PredExp.stringEqual;
import static com.aerospike.client.query.PredExp.stringValue;
import static com.nosqldriver.aerospike.sql.query.QueryHolder.BIN_NAME_DOES_NOT_EXIST;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

public class BinaryOperation {
    private Statement statement;
    private String table;
    private String column;
    private List<Object> values = new ArrayList<>(2);

    @VisibleForPackage
    static class PrimaryKeyEqualityPredicate implements Predicate<ResultSet> {
        private final Key key;
        private final boolean eq;

        PrimaryKeyEqualityPredicate(Key key, boolean eq) {
            this.key = key;
            this.eq = eq;
        }

        @Override
        public boolean test(ResultSet rs) {
            try {
                return Objects.equals(key, rs.getObject("PK")) == eq;
            } catch (SQLException e) {
                SneakyThrower.sneakyThrow(e);
                throw new IllegalStateException(); // This cannot happen because previous line in fact throwws exception. This line is written to satisfy compiler.
            }
        }
    }

    public BinaryOperation() {
    }

    private BinaryOperation(Statement statement, String table, String column, List<Object> values) {
        this.statement = statement;
        this.table = table;
        this.column = column;
        this.values = values;
    }

    public enum Operator {
        EQ("=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                if (!operation.values.isEmpty()) {
                    Object value = operation.values.get(0);
                    if ("PK".equals(operation.column)) {
                        final Key key = createKey(value, queries);
                        queries.createPkQuery(operation.statement, key);
                    } else {
                        queries.setFilter(createEqFilter(value, operation.column), operation.column);
                    }
                }
                return queries;
            }


            private Filter createEqFilter(Object value, String column) {
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
        NE("!=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                if (!operation.values.isEmpty() && "PK".equals(operation.column)) {
                    queries.createScanQuery(operation.statement, new PrimaryKeyEqualityPredicate(createKey(operation.values.get(0), queries), false));
                }
                return queries;
            }
        },
        NEQ("<>") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                return NE.update(queries, operation);
            }
        },
        GT(">") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = asList(((Number) operation.values.get(0)).longValue() + 1, Long.MAX_VALUE);
                return BETWEEN.update(queries, new BinaryOperation(operation.statement, operation.table, operation.column, values));
            }
        },
        GE(">=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = asList(operation.values.get(0), Long.MAX_VALUE);
                return BETWEEN.update(queries, new BinaryOperation(operation.statement, operation.table, operation.column, values));
            }
        },
        LT("<") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = asList(Long.MIN_VALUE, ((Number) operation.values.get(0)).longValue() - 1);
                return BETWEEN.update(queries, new BinaryOperation(operation.statement, operation.table, operation.column, values));
            }
        },
        LE("<=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = asList(Long.MIN_VALUE, operation.values.get(0));
                return BETWEEN.update(queries, new BinaryOperation(operation.statement, operation.table, operation.column, values));
            }
        },
        BETWEEN("BETWEEN") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                queries.setFilter(Filter.range(operation.column, ((Number) operation.values.get(0)).longValue(), ((Number) operation.values.get(1)).longValue()), operation.column);
                return queries;
            }
        },
        IN("IN") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                if ("PK".equals(operation.column)) {
                    queries.createPkBatchQuery(operation.statement, operation.values.stream().map(v -> createKey(v, queries)).toArray(Key[]::new));
                } else {
                    int nValues = operation.values.size();
                    QueryHolder qh = queries.queries(operation.getTable());
                    if (nValues == 0) {
                        prefix(BIN_NAME_DOES_NOT_EXIST, BIN_NAME_DOES_NOT_EXIST).forEach(qh::addPredExp);
                    } else {
                        operation.values.stream().map(v -> prefix(operation.column, v)).flatMap(List::stream).forEach(qh::addPredExp);
                        if (nValues > 1) {
                            qh.addPredExp(or(nValues));
                        }
                    }
                }
                return queries;
            }

            private List<PredExp> prefix(String column, Object value) {
                final List<PredExp> prefixes;
                if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
                    prefixes = asList(integerBin(column), integerValue(((Number) value).longValue()), integerEqual());
                } else if (value instanceof Date || value instanceof Calendar) {
                    Calendar calendar = value instanceof Calendar ? (Calendar)value : calendar((Date)value);
                    prefixes = asList(integerBin(column), integerValue(calendar), integerEqual());
                } else {
                    prefixes = asList(stringBin(column), stringValue(value == null ? null : value.toString()), stringEqual());
                }
                return prefixes;
            }


            private Calendar calendar(Date date) {
                Calendar c = Calendar.getInstance();
                c.setTime(date);
                return c;
            }
        },
        AND("AND", false) {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                return queries;
            }
        },
        OR("OR", false) {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                return queries;
            }
        },
        ;

        private static Map<String, Operator> operators = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        static {
            operators.putAll(Arrays.stream(Operator.values()).collect(toMap(e -> e.operator, e -> e)));
        }
        private final String operator;
        private final boolean requiresColumn;

        Operator(String operator) {
            this(operator, true);
        }

        Operator(String operator, boolean requiresColumn) {
            this.operator = operator;
            this.requiresColumn = requiresColumn;
        }

        public abstract QueryHolder update(QueryHolder queries, BinaryOperation operation);
        public static Optional<Operator> find(String op) {
            return ofNullable(operators.get(op));
        }

        public boolean doesRequireColumn() {
            return requiresColumn;
        }

        protected Key createKey(Object value, QueryHolder queries) {
            return KeyFactory.createKey(queries.getSchema(), queries.getSetName(), value);
        }

        public String operator() {
            return operator;
        }
    }

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getColumn() {
        return column;
    }

    public void addValue(Object value) {
        values.add(value);
    }

    public void clear() {
        column = null;
        values.clear();
    }
}
