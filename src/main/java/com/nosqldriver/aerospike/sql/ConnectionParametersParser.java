package com.nosqldriver.aerospike.sql;

import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;

import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

class ConnectionParametersParser {
    private static final Pattern AS_JDBC_URL = Pattern.compile("^jdbc:aerospike:([^/?]+)");
    private static final Pattern AS_JDBC_SCHEMA = Pattern.compile("/([^?]+)");

    ClientPolicy policy(String url, Properties info) {
        ClientPolicy clientPolicy = new ClientPolicy();

        clientInfo(url, info).forEach((key, value) -> {
            try {
                ClientPolicy.class.getField((String)key).set(clientPolicy, value);
            } catch (ReflectiveOperationException e1) {
                // ignore it; this property does not belong to ClientPolicy
            }
        });

        return clientPolicy;
    }

    Host[] hosts(String url) {
        Matcher m = AS_JDBC_URL.matcher(url);
        if (!m.find()) {
            throw new IllegalArgumentException("Cannot parse URL " + url);
        }
        return Arrays.stream(m.group(1).split(","))
                .map(p -> p.split(":")).map(a -> a.length > 1 ? a : new String[]{a[0], "3000"})
                .map(hostPort -> new Host(hostPort[0], Integer.parseInt(hostPort[1])))
                .toArray(Host[]::new);
    }

    String schema(String url) {
        Matcher m = AS_JDBC_SCHEMA.matcher(url);
        return m.find() ? m.group(1) : null;
    }


//         * <li>ApplicationName  -       The name of the application currently utilizing
//         *                                                      the connection</li>
//            * <li>ClientUser               -       The name of the user that the application using
//         *                                                      the connection is performing work for.  This may
//         *                                                      not be the same as the user name that was used
//         *                                                      in establishing the connection.</li>
//            * <li>ClientHostname   -       The hostname of the computer the application
    Properties clientInfo(String url, Properties info) {
        Properties all = new Properties(info);
        int questionPos = url.indexOf('?');
        if (questionPos > 0 && questionPos < url.length() - 1) {
            Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                String[] kv = p.split("=");
                if (kv.length > 1) {
                    all.setProperty(kv[0], kv[1]);
                }
            });
        }
        return all;
    }

    <T> T initProperties(T object, Properties props) {
        @SuppressWarnings("unchecked")
        Class<T> clazz = (Class<T>)object.getClass();
        props.forEach((key, value) -> {
            try {
                clazz.getField((String)key).set(object, value);
            } catch (ReflectiveOperationException e1) {
                // ignore it; this property does not belong to ClientPolicy
            }
        });

        return object;
    }

    Properties subProperties(Properties properties, String prefix) {
        String filter = prefix.endsWith(".") ? prefix : prefix + ".";
        int prefixLength = filter.length();
        Properties result = new Properties();
        result.putAll(properties.entrySet().stream()
                .filter(e -> ((String)e.getKey()).startsWith(filter))
                .collect(Collectors.toMap(e -> ((String)e.getKey()).substring(prefixLength), Map.Entry::getValue)));
        return result;
    }

    Collection<String> indexesParser(String infos) {
        return Arrays.stream(infos.split(";")).map(info -> new StringReader(info.replace(":", "\n"))).map(r -> {
            Properties props = new Properties();
            try {
                props.load(r);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return props;
        }).map(p -> indexKey(p.getProperty("ns"), p.getProperty("set"), p.getProperty("bin"))).collect(Collectors.toSet());
    }

    private String indexKey(String namespace, String set, String bin) {
        return String.join(".", namespace, set, bin);
    }
}
