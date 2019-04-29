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


    public JoinedResultSetInvocationHandler(ResultSet resultSet, List<JoinHolder> joinHolders, String schema, String[] names, String[] aliases) {
        super(NEXT | METADATA | GET_NAME | GET_INDEX, resultSet, schema, names, aliases);
        this.joinHolders = joinHolders;
    }


    @Override
    protected boolean next() throws SQLException {
        SUPER:
        while (resultSet.next()) {
            resultSets.clear();
            for (JoinHolder jh : joinHolders) {
                ResultSet rs = jh.getResultSetRetriver().apply(resultSet);
                if (!jh.isSkipIfMissing() && (rs == null || !rs.next())) {
                    continue SUPER;
                }
                resultSets.add(rs);
            }
            return true;
        }

        return false;
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
                Object value = rs.getObject(alias);
                if (value != null) {
                    return cast(value, type);
                }
            }
            return null;
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }

    }
}
