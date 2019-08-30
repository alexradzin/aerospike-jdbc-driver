package com.nosqldriver.util;

import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;

import static com.nosqldriver.util.SneakyThrower.sneakyThrow;
import static java.lang.String.format;

public class DateParser {
    private static final DateFormat[] formats = new DateFormat[] {
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm"),
            new SimpleDateFormat("yyyy-MM-dd"),
    };

    public static Date date(String s) {
        Optional<Date> date = Arrays.stream(formats).map(f -> parse(f, s)).filter(Objects::nonNull).findFirst();

        if (!date.isPresent()) {
            sneakyThrow(new SQLException(format("Cannot parse %s as date", s)));
        }

        return date.get();
    }

    private static Date parse(DateFormat fmt, String s) {
        try {
            return fmt.parse(s);
        } catch (ParseException e) {
            return null;
        }
    }
}
