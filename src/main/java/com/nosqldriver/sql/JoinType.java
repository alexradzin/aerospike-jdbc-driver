package com.nosqldriver.sql;

import net.sf.jsqlparser.statement.select.Join;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;

public enum JoinType {
    SIMPLE(Join::isSimple, true),
    INNER(Join::isInner, true),
    OUTER(Join::isOuter, false),
    RIGHT(Join::isRight, false),
    LEFT(Join::isLeft, false),
    FULL(Join::isFull, false),
    CROSS(Join::isCross, false),
    ;

    private final Predicate<Join> isA;
    private final boolean skipMissing;

    JoinType(Predicate<Join> isA, boolean skipMissing) {
        this.isA = isA;
        this.skipMissing = skipMissing;
    }

    public static JoinType[] getTypes(Join join) {
        return Arrays.stream(values()).filter(t -> t.isA.test(join)).toArray(JoinType[]::new);
    }

    public static boolean skipIfMissing(JoinType[] types) throws IllegalArgumentException {
        if (types.length == 0) {
            return true; // it is implicitly defined inner join
        }
        Set<Boolean> skip = Arrays.stream(types).map(t -> t.skipMissing).collect(Collectors.toSet());
        if (skip.size() > 1) {
            throw new IllegalArgumentException(format("Incompatable join typpes %s", Arrays.toString(types)));
        }
        return skip.iterator().next();
    }
}