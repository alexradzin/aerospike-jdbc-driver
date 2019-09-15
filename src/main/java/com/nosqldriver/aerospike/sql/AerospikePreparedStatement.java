package com.nosqldriver.aerospike.sql;

import com.aerospike.client.IAerospikeClient;
import com.nosqldriver.aerospike.sql.query.QueryContainer;
import com.nosqldriver.sql.ByteArrayBlob;
import com.nosqldriver.sql.SimpleParameterMetaData;
import com.nosqldriver.sql.StringClob;
import com.nosqldriver.util.IOUtils;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

public class AerospikePreparedStatement extends AerospikeStatement implements PreparedStatement {
    private  final String sql;
    private Object[] parameterValues;
    private final QueryContainer<ResultSet> queryPlan;

    public AerospikePreparedStatement(IAerospikeClient client, Connection connection, AtomicReference<String> schema, AerospikePolicyProvider policyProvider, String sql) throws SQLException {
        super(client, connection, schema, policyProvider);
        this.sql = sql;
        int n = parametersCount(sql);
        parameterValues = new Object[n];
        Arrays.fill(parameterValues, Optional.empty());
        queryPlan = new AerospikeQueryFactory(schema.get(), policyProvider, indexes).createQueryPlan(sql);
    }

    private int parametersCount(String sql) {
        int count = 0;
        boolean intoConstantt = false;
        for (char c : sql.toCharArray()) {
            if (c == '\'') {
                intoConstantt = !intoConstantt;
            }
            if (!intoConstantt && c == '?') {
                count++;
            }
        }
        return count;
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(sql);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setObject(parameterIndex, null);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setObject(parameterIndex, x ? 1 : 0);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long)length);
    }

    @Override
    @Deprecated
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream() is deprecated");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBlob(parameterIndex, x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        Arrays.fill(parameterValues, Optional.empty());
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        if (parameterIndex <= 0 || parameterIndex > parameterValues.length) {
            throw new IllegalArgumentException(parameterValues.length == 0 ?
                    "Current SQL statement does not have parameters" :
                    format("Wrong parameter index. Expected from %d till %d", 1, parameterValues.length));
        }
        parameterValues[parameterIndex - 1] = x;
    }

    @Override
    public boolean execute() throws SQLException {
        return execute(sql);
    }

    @Override
    public void addBatch() throws SQLException {
        super.addBatch(sql);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob blob) throws SQLException {
        setBytes(parameterIndex, blob.getBytes(1, (int)blob.length()));
    }

    @Override
    public void setClob(int parameterIndex, Clob clob) throws SQLException {
        setString(parameterIndex, clob.getSubString(1, (int)clob.length()));
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException(); // TODO: think how to support this: extract metadata without code duplication and query execution.
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL url) throws SQLException {
        setString(parameterIndex, url != null ? url.toString() : null);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return new SimpleParameterMetaData(parameterValues.length);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob clob) throws SQLException {
        setString(parameterIndex, clob.getSubString(1, (int)clob.length()));
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            String result = IOUtils.toString(reader);
            if (result.length() != length) {
                throw new SQLException(format("Unexpected data length: expected %s but was %d", length, result.length()));
            }
            setObject(parameterIndex, result);
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        byte[] bytes = new byte[(int) length];
        DataInputStream dis = new DataInputStream(inputStream);
        try {
            dis.readFully(bytes);
            if (inputStream.read() !=-1) {
                throw new SQLException(format("Source contains more bytes than required %d", length));
            }
            setBytes(parameterIndex, bytes);
        } catch (EOFException e) {
            throw new SQLException(format("Source contains less bytes than required %d", length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setClob(parameterIndex, new InputStreamReader(x), length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBlob(parameterIndex, x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setClob(parameterIndex, new InputStreamReader(x));
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setBlob(parameterIndex, x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setNClob(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            setClob(parameterIndex, new StringClob(IOUtils.toString(reader)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            setBlob(parameterIndex, new ByteArrayBlob(IOUtils.toByteArray(inputStream)));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

    @Override
    protected AerospikeQueryFactory createQueryFactory() {
        return new AerospikeQueryFactory(schema.get(), policyProvider, indexes) {
            @Override  QueryContainer<ResultSet> createQueryPlan(String sql) throws SQLException {
                QueryContainer<ResultSet> qc = Objects.equals(AerospikePreparedStatement.this.sql, sql) ? AerospikePreparedStatement.this.queryPlan : super.createQueryPlan(sql);
                qc.setParameters(parameterValues);
                return qc;
            }
        };
    }
}
