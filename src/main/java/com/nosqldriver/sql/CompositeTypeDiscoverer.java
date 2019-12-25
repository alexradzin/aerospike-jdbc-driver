package com.nosqldriver.sql;

import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;

public class CompositeTypeDiscoverer implements TypeDiscoverer {
    private final Collection<TypeDiscoverer> discoverers;

    public CompositeTypeDiscoverer(TypeDiscoverer ... discoverers) {
        this(asList(discoverers));
    }


    public CompositeTypeDiscoverer(Collection<TypeDiscoverer> discoverers) {
        this.discoverers = discoverers;
    }

    @Override
    public List<DataColumn> discoverType(List<DataColumn> columns) {
        discoverers.forEach(d -> d.discoverType(columns));
        return columns;
    }
}
