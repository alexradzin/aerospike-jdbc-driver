package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.Key;
import com.aerospike.client.query.Filter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.lang.String.format;

 public class BinaryOperation {
    private String column;
    private List<Object> values = new ArrayList<>(2);

    public BinaryOperation() {
    }

    public BinaryOperation(String column, List<Object> values) {
        this.column = column;
        this.values = values;
    }

    public enum Operator {
        EQ("=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                Object value = operation.values.get(0);
                if ("PK".equals(operation.column)) {
                    final Key key = createKey(value, queries);
                    queries.createPkQuery(key);
                } else {
                    queries.setFilter(createEqFilter(value, operation.column), operation.column);
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
        GT(">") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = Arrays.asList(((Number)operation.values.get(0)).longValue() + 1, Long.MAX_VALUE);
                return BETWEEN.update(queries, new BinaryOperation(operation.column, values));
            }
        },
        GE(">=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = Arrays.asList(operation.values.get(0), Long.MAX_VALUE);
                return BETWEEN.update(queries, new BinaryOperation(operation.column, values));
            }
        },
        LT("<") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = Arrays.asList(Long.MIN_VALUE, ((Number)operation.values.get(0)).longValue() - 1);
                return BETWEEN.update(queries, new BinaryOperation(operation.column, values));
            }
        },
        LE("<=") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                List<Object> values = Arrays.asList(Long.MIN_VALUE, operation.values.get(0));
                return BETWEEN.update(queries, new BinaryOperation(operation.column, values));
            }
        },
        BETWEEN("BETWEEN") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                queries.setFilter(Filter.range(operation.column, ((Number)operation.values.get(0)).longValue(), ((Number)operation.values.get(1)).longValue()), operation.column);
                return queries;
            }
        },
        IN("IN") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                if ("PK".equals(operation.column)) {
                    queries.createPkBatchQuery(operation.values.stream().map(v -> createKey(v, queries)).toArray(Key[]::new));
                } else {
                    throw new IllegalArgumentException("select ... from ... where IN is not supported with secondary index");
                    // TODO: think how to support this anyway.
                }
                return queries;
            }
        },
        AND("AND") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
                return queries;
            }
        },
        OR("OR") {
            @Override
            public QueryHolder update(QueryHolder queries, BinaryOperation operation) {
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

        public abstract QueryHolder update(QueryHolder queries, BinaryOperation operation);
        public static Operator find(String op) {
            return Optional.ofNullable(operators.get(op)).orElseThrow(() -> new IllegalArgumentException(op));
        }

        protected Key createKey(Object value, QueryHolder queries) {
            final Key key;
            final String schema = queries.getSchema();
            if (value instanceof Long) {
                key = new Key(schema, queries.getSetName(), (Long)value);
            } else if (value instanceof Integer) {
                key = new Key(schema, queries.getSetName(), (Integer)value);
            } else if (value instanceof Number) {
                key = new Key(schema, queries.getSetName(), ((Number)value).intValue());
            } else if (value instanceof String) {
                key = new Key(schema, queries.getSetName(), (String)value);
            } else {
                throw new IllegalArgumentException(format("Filter by %s is not supported right now. Use either number or string", value == null ? null : value.getClass()));
            }
            return key;
        }
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
