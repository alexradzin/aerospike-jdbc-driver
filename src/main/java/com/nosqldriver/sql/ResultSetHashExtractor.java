package com.nosqldriver.sql;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Date;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Optional.ofNullable;

public class ResultSetHashExtractor implements Function<ResultSet, byte[]> {
    private final Predicate<String> columnNameFilter;

    public ResultSetHashExtractor() {
        this(name -> true);
    }

    public ResultSetHashExtractor(Predicate<String> columnNameFilter) {
        this.columnNameFilter = columnNameFilter;
    }


    @Override
    public byte[] apply(ResultSet rs) {
        try {
            return impl(rs);
        } catch (SQLException | IOException | NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }


    private byte[] impl(ResultSet rs) throws SQLException, IOException, NoSuchAlgorithmException {
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream daos = new DataOutputStream(baos);

        for (int i = 1; i <= n; i++) {
            String label = md.getColumnLabel(i);
            if(!columnNameFilter.test(label)) {
                continue;
            }

            int type = md.getColumnType(i);
            switch(type) {
                case Types.BOOLEAN: daos.writeBoolean(rs.getBoolean(i)); break;
                case Types.SMALLINT: daos.writeShort(rs.getShort(i)); break;
                case Types.INTEGER: daos.writeInt(rs.getInt(i)); break;
                case Types.BIGINT: daos.writeLong(rs.getLong(i)); break;
                case Types.FLOAT: daos.writeFloat(rs.getFloat(i)); break;
                case Types.DOUBLE: daos.writeDouble(rs.getDouble(i)); break;
                case Types.VARCHAR: daos.writeChars(rs.getString(i)); break;
                case Types.DATE: daos.writeLong(ofNullable(rs.getDate(i)).map(Date::getTime).orElse(0L)); break;
                case Types.TIME: daos.writeLong(ofNullable(rs.getTime(i)).map(Date::getTime).orElse(0L)); break;
                case Types.TIMESTAMP: daos.writeLong(ofNullable(rs.getTimestamp(i)).map(Date::getTime).orElse(0L)); break;
                default: throw new IllegalArgumentException("Unsupported type " + type);
            }
        }
        daos.flush();
        daos.close();

        MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        messageDigest.update(baos.toByteArray());
        return messageDigest.digest();
    }
}
