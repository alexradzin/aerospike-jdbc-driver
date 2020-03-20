package com.nosqldriver.util;

import com.nosqldriver.sql.DataColumn;

import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

public class Deserializer {
    private static final Pattern DESERIALIZE_ARG = Pattern.compile("deserialize\\s*\\(\\s*(.*?)\\s*\\)");

    /**
     * Invoked from Javascript funtion {@code deserialize()} implemented in {@code functions.js}
     * @param any
     * @param cdm
     * @param column
     * @return
     */
    public static Object deserialize(Object any, CustomDeserializerManager cdm, DataColumn column) {
        return getDeserializer(any, cdm, column).map(d -> d.apply(any)).orElse(null);
    }

    public static Optional<Function<Object, Object>> getDeserializer(Object any, CustomDeserializerManager cdm, DataColumn column) {
        return cdm.getDeserializer(
                format("%s:%s:%s",
                        column.getCatalog(),
                        column.getTable(),
                        getFunctionArgument(column.getExpression())
                ),
                Optional.ofNullable(any).map(Object::getClass).orElse(null)
        ).map(d  -> ((Function<Object, Object>) d));
    }

    public static String getFunctionArgument(String expression) {
        Matcher m = DESERIALIZE_ARG.matcher(expression);
        if (!m.find()) {
            throw new IllegalArgumentException(expression);
        }
        return m.group(1);
    }
}
