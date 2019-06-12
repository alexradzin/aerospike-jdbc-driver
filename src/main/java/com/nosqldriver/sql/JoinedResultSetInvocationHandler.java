package com.nosqldriver.sql;

import com.nosqldriver.aerospike.sql.query.JoinHolder;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
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
            if (columnTags(resultSet.getMetaData()).contains(alias)) {
                Object value = resultSet.getObject(alias);
                // Metadata of the main result set contains all fields including those that in fact are retrieved from
                // joined tables. So, we have to perform null check and try to retrieve the data from other result sets
                // if it is null here.
                if (value != null) {
                    return cast(value, type);
                }
            }

            for (ResultSet rs : resultSets) {
                if (columnTags(rs.getMetaData()).contains(alias)) {
                    return cast(rs.getObject(alias), type);
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
