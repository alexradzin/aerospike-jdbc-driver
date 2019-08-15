package com.nosqldriver.sql;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

public class ChainedResultSetWrapper extends ResultSetWrapper {
    private final List<ResultSet> resultSets;
    private ListIterator<ResultSet> lit;
    private boolean beforeFirst = true;
    private boolean afterLast = false;

    public ChainedResultSetWrapper(List<ResultSet> resultSets, boolean indexByName) {
        this(resultSets, Collections.emptyList(), indexByName);
    }

    private ChainedResultSetWrapper(List<ResultSet> resultSets, List<DataColumn> columns, boolean indexByName) {
        super(null, columns, indexByName);
        this.resultSets = resultSets;
        lit = resultSets.listIterator();
        rs = resultSets.isEmpty() ? new ListRecordSet(null, null, Collections.emptyList(), Collections.emptyList()) : lit.next();
    }


    @Override
    public boolean next() throws SQLException {
        while (true) {
            if (rs.next()) {
                beforeFirst = false;
                return true;
            } else if(lit.hasNext()) {
                rs = lit.next();
                if (rs.next()) {
                    beforeFirst = false;
                    return true;
                }
            } else {
                afterLast = false;
                return false;
            }
        }
    }

    @Override
    public boolean first() throws SQLException {
        if (resultSets.isEmpty()) {
            beforeFirst = true;
            return false;
        }
        lit = resultSets.listIterator();
        rs = lit.next();
        boolean res =  rs.next();
        if (!res) {
            beforeFirst = true;
        }
        return res;
    }

    @Override
    public boolean last() throws SQLException {
        int n = resultSets.size();
        for (int i = n - 1; i >= 0 ; i--) {
            ResultSet rs = resultSets.get(i);
            if (rs.last()) {
                return true;
            }
        }
        afterLast = true;
        return false;
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return beforeFirst;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        return !lit.hasPrevious() && rs.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return !lit.hasNext() && rs.isLast();
    }

    @Override
    public void beforeFirst() throws SQLException {
        lit = resultSets.listIterator();
        rs = resultSets.isEmpty() ? new ListRecordSet(null, null, Collections.emptyList(), Collections.emptyList()) : lit.next();
    }

    @Override
    @SuppressWarnings("StatementWithEmptyBody") // for loop is used to reach the last element
    public void afterLast() throws SQLException {
        for (; lit.hasNext(); rs = lit.next());
        rs.afterLast();
    }
}
