package com.nosqldriver.sql;

import java.util.List;

public interface TypeDiscoverer {
    List<DataColumn> discoverType(List<DataColumn> columns);
}
