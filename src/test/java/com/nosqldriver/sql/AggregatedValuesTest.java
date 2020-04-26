package com.nosqldriver.sql;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import static com.nosqldriver.sql.DataColumn.DataColumnRole.AGGREGATED;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.DATA;
import static com.nosqldriver.sql.DataColumn.DataColumnRole.GROUP;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


class AggregatedValuesTest {
    private List<DataColumn> aggregationColumns = asList(
            AGGREGATED.create("test", null, "count(*)", null),
            AGGREGATED.create("test", null, "sum(n)", null),
            AGGREGATED.create("test", null, "avg(n)", null),
            AGGREGATED.create("test", null, "min(n)", null),
            AGGREGATED.create("test", null, "max(n)", null),
            AGGREGATED.create("test", null, "sumsqs(n)", null)
    );

    @Test
    void empty() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);
        assertTrue(new AggregatedValues(rs, Collections.emptyList()).read().isEmpty());
    }

    @Test
    void emptyAggregationFunctions() throws SQLException {
        ResultSet rs = mock(ResultSet.class);
        when(rs.next()).thenReturn(false);
        assertTrue(new AggregatedValues(rs, aggregationColumns).read().isEmpty());
    }

    @Test
    void oneCount() {
        one("count(*)", 1, 1L);
        one("count(*)", 0, 1L);
        one("count(*)", 2, 1L);
    }

    @Test
    void oneSum() {
        one("sum(n)", 1, 1);
        one("sum(n)", 0, 0);
        one("sum(n)", 2, 2);
    }


    private <E> void one(String function, int value, E expectedValue) {
        List<DataColumn> aggregationColumns = singletonList(
                AGGREGATED.create("test", "data", function, function)
        );

        ResultSet rs = new ListRecordSet(null, "", "", singletonList(DATA.create("test", "data", "n", "n")), singletonList(singletonList(value)));
        List<?> expected = singletonList(singletonList(expectedValue));
        List<?> actual = new AggregatedValues(rs, aggregationColumns).read();
        assertEquals(expected, actual);
    }

    @Test
    void allFunctionsOneLine() {
        oneColumn(singletonList(singletonList(1)), singletonList(asList(1, 1, 1, 1, 1, 1)));
    }

    @Test
    void allFunctionsSeveralLines() {
        oneColumn(
                asList(singletonList(10), singletonList(1), singletonList(5)),
                singletonList(asList(3, 16, 16.0/3.0, 1, 10, 100 + 1 + 25)));
    }


    @Test
    void allFunctionsSeveralColumnsSeveralLines() {
        int year = 2020;
        severalColumns(
                asList(DATA.create("test", "data", "year", null), DATA.create("test", "data", "age", null)),
                asList(
                        AGGREGATED.create("test", null, "count(*)", null),
                        AGGREGATED.create("test", null, "sum(year)", null),
                        AGGREGATED.create("test", null, "sum(age)", null),
                        AGGREGATED.create("test", null, "avg(year)", null),
                        AGGREGATED.create("test", null, "avg(age)", null),
                        AGGREGATED.create("test", null, "min(year)", null),
                        AGGREGATED.create("test", null, "min(age)", null),
                        AGGREGATED.create("test", null, "max(year)", null),
                        AGGREGATED.create("test", null, "max(age)", null),
                        AGGREGATED.create("test", null, "sumsqs(year)", null),
                        AGGREGATED.create("test", null, "sumsqs(age)", null)
                ),
                asList(asList(1940, year - 1940), asList(1942, year - 1942), asList(1943, year - 1943), asList(1940, year - 1940)),
                singletonList(asList(4, 7765, 315, 7765.0/4, 315.0/4, 1940, 77, 1943, 80, 15073813, 24813)));
    }

    @Test
    void groupByCountGroup() {
        severalColumns(
                asList(DATA.create("test", "data", "year", null), DATA.create("test", "data", "first_name", null)),
                asList(
                        AGGREGATED.create("test", "data", "count(*)", null),
                        GROUP.create("test", "data", "year", null)
                ),
                asList(asList(1940, "John"), asList(1942, "Paul"), asList(1943, "George"), asList(1940, "Ringo")),
                asList(asList(2, 1940), asList(1, 1942), asList(1, 1943)));
    }

    @Test
    void groupByGroupCount() {
        severalColumns(
                asList(DATA.create("test", "data", "year", null), DATA.create("test", "data", "first_name", null)),
                asList(
                        GROUP.create("test", "data", "year", null),
                        AGGREGATED.create("test", "data", "count(*)", null)
                ),
                asList(asList(1940, "John"), asList(1942, "Paul"), asList(1943, "George"), asList(1940, "Ringo")),
                asList(asList(1940, 2), asList(1942, 1), asList(1943, 1)));
    }

    @Test
    void groupByGroupCountSum() {
        severalColumns(
                asList(
                        DATA.create("test", "data", "year", null),
                        DATA.create("test", "data", "first_name", null)
                ),
                asList(
                        GROUP.create("test", "data", "year", null),
                        AGGREGATED.create("test", "data", "count(*)", null),
                        AGGREGATED.create("test", "data", "sum(year)", null)
                ),
                asList(asList(1940, "John"), asList(1942, "Paul"), asList(1943, "George"), asList(1940, "Ringo")),
                asList(asList(1940, 2, 1940 * 2), asList(1942, 1, 1942), asList(1943, 1, 1943)));
    }

    @Test
    void sum() {
        long now = System.currentTimeMillis();
        severalColumns(
            asList(
                    DATA.create("test", "data", "name", null),
                    DATA.create("test", "data", "number", null)
            ),
            asList(
                    GROUP.create("test", "data", "name", null),
                    AGGREGATED.create("test", "data", "sum(number)", null)
            ),
            asList(asList("x", 1), asList("x", 2), asList("y", now)),
            asList(asList("x", 3L), asList("y", now)));
    }



    private void oneColumn(Iterable<List<?>> data, List<?> expected) {
        severalColumns(singletonList(DATA.create("test", "data", "n", "n")), aggregationColumns, data, expected);
    }

    private void severalColumns(List<DataColumn> columns, List<DataColumn> aggregationColumns, Iterable<List<?>> data, List<?> expected) {
        ResultSet rs = new ListRecordSet(null, "", "", columns, data);
        List<?> actual = new AggregatedValues(rs, aggregationColumns).read();
        assertEquals(expected, actual);
   }
}