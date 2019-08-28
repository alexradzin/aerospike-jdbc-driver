package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;

import java.util.Calendar;

public class PredExpValuePlaceholder extends FakePredExp {
    private final int index;

    public PredExpValuePlaceholder(int index) {
        this.index = index;
    }

    PredExp createPredExp(Object val) {
        if(val instanceof Byte || val instanceof Short || val instanceof Integer || val instanceof Long) {
            return PredExp.integerValue(((Number)val).longValue());
        }
        if (val instanceof Calendar) {
            return PredExp.integerValue((Calendar)val);
        }
        if(val instanceof String) {
            return PredExp.stringValue((String)val);
        }
        throw new IllegalArgumentException("" + val);
    }

    public int getIndex() {
        return index;
    }
}
