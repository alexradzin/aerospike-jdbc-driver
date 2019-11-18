package com.nosqldriver.sql;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class CompositeTypeDiscoverer implements TypeDiscoverer {
    private final Collection<TypeDiscoverer> discoverers;

    public CompositeTypeDiscoverer(TypeDiscoverer ... discoverers) {
        this(Arrays.asList(discoverers));
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
