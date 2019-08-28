package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;
import com.nosqldriver.VisibleForPackage;

@VisibleForPackage
abstract class FakePredExp extends PredExp {
    @Override
    public int estimateSize() {
        throw new IllegalStateException();
    }

    @Override
    public int write(byte[] buf, int offset) {
        throw new IllegalStateException();
    }
}
