package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;
import com.nosqldriver.util.SneakyThrower;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.nosqldriver.sql.SqlLiterals.operatorKey;
import static com.nosqldriver.sql.SqlLiterals.predExpOperators;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class PredExpValuePlaceholder extends FakePredExp {
    private static final Collection<Class> intTypes = new HashSet<>(asList(Byte.class, Short.class, Integer.class, Long.class, byte.class, short.class, int.class, long.class));
    private final int index;

    public PredExpValuePlaceholder(int index) {
        this.index = index;
    }

    private PredExp createPredExp(Object val) {
        return ValueHolderPredExp.create(val);
    }

    public int getIndex() {
        return index;
    }

    Class updatePreExp(List<PredExp> predExps, int i, Object parameter) {
        int j = i;

        int inOpIndex = -1;
        for (; j < predExps.size(); j++) {
            PredExp pe = predExps.get(j);
            if (!(pe instanceof PredExpValuePlaceholder)) {
                Class clazz = pe.getClass();
                if ("com.aerospike.client.query.PredExp$AndOr".equals(clazz.getName())) {
                    int op = getValue(clazz, pe, "op");
                    if (op == 2) { // OR
                        inOpIndex = j;
                    }
                    break;
                }
            }
        }

        final Class type;
        if (parameter instanceof List) {
            type = ((List) parameter).size() == 0 ? String.class : ((List) parameter).get(0).getClass();
        } else if (parameter != null && parameter.getClass().isArray()) {
            type = parameter.getClass().getComponentType();
        } else {
            type = parameter == null ? String.class : parameter.getClass();
        }
        if (predExps.get(i - 1) instanceof ColumnRefPredExp) {
            String binName = ((ColumnRefPredExp) predExps.get(i - 1)).getName();
            predExps.set(i - 1, createBinPredExp(binName, type));
        }


        if (inOpIndex < 0) {
            predExps.set(i, createPredExp(parameter));
        } else {
            final List<PredExp> predicates;
            final int n;
            if (parameter instanceof List) {
                predicates = getMultiplePredicates(predExps.get(i - 1), parameter, p -> ((List) p).size(), (p, index) -> ((List) p).get(index));
                n = ((List) parameter).size();
            } else if (parameter != null && parameter.getClass().isArray()) {
                predicates = getMultiplePredicates(predExps.get(i - 1), parameter, Array::getLength, Array::get);
                n = Array.getLength(parameter);
            } else {
                predicates = Arrays.asList(createPredExp(parameter), predExpOperators.get(operatorKey(type, "=")).get());
                n = 1;
            }
            PredExp inOp = predExps.get(inOpIndex);
            int nexp = getValue(inOp.getClass(), inOp, "nexp");
            predExps.set(inOpIndex, PredExp.or(nexp + n));
            predExps.remove(i);
            predExps.addAll(i, predicates);
        }

        return type;
    }


    private List<PredExp> getMultiplePredicates(PredExp column, Object parameters, Function<Object, Integer> sizeGetter, BiFunction<Object, Integer, Object> elementGetter) {
        final List<PredExp> predicates;
        int n = sizeGetter.apply(parameters);
        predicates = new ArrayList<>();

        for (int k = 0; k < n; k++) {
            if (k > 0) {
                predicates.add(column);
            }
            Object p = elementGetter.apply(parameters, k);
            predicates.addAll(Arrays.asList(createPredExp(p), predExpOperators.get(operatorKey(p.getClass(), "=")).get()));
        }
        return predicates;
    }


    private <T> T getValue(Class clazz, Object obj, String name) {
        try {
            Field f = clazz.getDeclaredField(name);
            f.setAccessible(true);
            //noinspection unchecked
            return (T) f.get(obj);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    static PredExp createBinPredExp(String name, Class<?> type) {
        if (intTypes.contains(type)) {
            return PredExp.integerBin(name);
        }
        if (String.class.equals(type)) {
            return PredExp.stringBin(name);
        }
        if (java.sql.Array.class.isAssignableFrom(type) || type.isArray() || Collection.class.isAssignableFrom(type)) {
            return PredExp.listBin(name);
        }
        if (Map.class.isAssignableFrom(type)) {
            return PredExp.mapBin(name);
        }

        // TODO: add GeoJson

        return SneakyThrower.sneakyThrow(new SQLException(format("Cannot create where clause using type %s", type)));
    }
}
