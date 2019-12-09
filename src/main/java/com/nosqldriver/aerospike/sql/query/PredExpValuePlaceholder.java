package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;
import com.nosqldriver.VisibleForPackage;

import java.util.Calendar;

public class PredExpValuePlaceholder extends FakePredExp {
    private final int index;

    public PredExpValuePlaceholder(int index) {
        this.index = index;
    }

    @VisibleForPackage
    PredExp createPredExp(Object val) {
        return ValueHolderPredExp.create(val);
    }

    public int getIndex() {
        return index;
    }
}
