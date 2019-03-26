package com.nosqldriver.aerospike.sql;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class AerospikeDriver implements Driver {
    static {
        // Register the AerospikeDriver with DriverManager
        try {
            AerospikeDriver driverObj = new AerospikeDriver();
            DriverManager.registerDriver(driverObj);
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return new AerospikeConnection(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith("jdbc:aerospike:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        int questionPos = url.indexOf('?');
        Collection<DriverPropertyInfo> allInfo = new ArrayList<>();
        if (questionPos > 0 && questionPos < url.length() - 1) {
            Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                String[] kv = p.split("=");
                allInfo.add(new DriverPropertyInfo(kv[0], kv.length > 1 ? kv[1] : null));
            });
        }
        allInfo.addAll(info.entrySet().stream().map(e -> new DriverPropertyInfo((String)e.getKey(), (String)e.getValue())).collect(Collectors.toList()));
        return allInfo.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger(getClass().getName());
    }
}
