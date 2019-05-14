package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.query.JoinHolder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

import static com.nosqldriver.sql.TypeTransformer.cast;

public class JoinedResultSetInvocationHandler extends ResultSetInvocationHandler<ResultSet> {
    private final List<JoinHolder> joinHolders;
    private final List<ResultSet> resultSets = new ArrayList<>();
    private boolean moveMainResultSet = true;
    private boolean hasMore = true;


    public JoinedResultSetInvocationHandler(ResultSet resultSet, List<JoinHolder> joinHolders, String schema, String[] names, String[] aliases) {
        super(NEXT | METADATA | GET_NAME | GET_INDEX, resultSet, schema, names, aliases);
        this.joinHolders = joinHolders;
    }


    @Override
    protected boolean next() throws SQLException {
        if (moveMainResultSet) {
            SUPER:
            while (resultSet.next()) {
                resultSets.clear();
                moveMainResultSet = false;
                for (JoinHolder jh : joinHolders) {
                    ResultSet rs = jh.getResultSetRetriver().apply(resultSet);
                    boolean hasNext = rs != null && rs.next();
                    if (jh.isSkipIfMissing() && !hasNext) {
                        continue SUPER;
                    }
                    resultSets.add(rs);
                }
                moveMainResultSet = false;
                hasMore = true;
                return true;
            }
            moveMainResultSet = false;
            hasMore = false;
            return false;
        } else if(hasMore) {
            boolean allDone = true;
            for (int i = 0; i < joinHolders.size(); i++) {
                ResultSet rs = resultSets.get(i);
                boolean hasNext = rs.next();
                if (hasNext) {
                    allDone = false;
                }
                if (!hasNext && joinHolders.get(i).isSkipIfMissing()) {
                    moveMainResultSet = true;
                    break;
                }
            }
            moveMainResultSet = allDone;
            return !moveMainResultSet || next();
        } else {
            return false;
        }
    }

    @Override
    protected ResultSetMetaData getMetadata() throws SQLException {
        Collection<ResultSetMetaData> metadata = new ArrayList<>();
        metadata.add(resultSet.getMetaData());
        for (ResultSet rs : resultSets) {
            metadata.add(rs.getMetaData());
        }
        return new CompositeResultSetMetaData(metadata);
    }

    @Override
    protected <T> T get(int index, Class<T> type) {
        try {
            return get(getMetadata().getColumnLabel(index), type);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }




    @Override
    protected <T> T get(String alias, Class<T> type) {
        try {
            Collection<ResultSet> allResultSets = new LinkedHashSet<>(resultSets.size() + 1);
            allResultSets.add(resultSet);
            allResultSets.addAll(resultSets);
            for (ResultSet rs : allResultSets) {
                if (columnTags(rs.getMetaData()).contains(alias)) {
                    Object value = rs.getObject(alias);
                    if (value != null) { //TODO: this if is relevant for the main result set only because its metadata holds all fileds. Think about better solution.
                        return cast(value, type);
                    }
                }
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    private Collection<String> columnTags(ResultSetMetaData md) throws SQLException {
        int columnCount = md.getColumnCount();
        Collection<String> labels = new ArrayList<>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            String label = md.getColumnLabel(i + 1);
            String tag = label != null ? label : md.getColumnName(i + 1);
            labels.add(tag);
        }
        return labels;
    }

}
