package com.nosqldriver.aerospike.sql;

import com.nosqldriver.sql.DriverPolicy;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public enum SpecialField {
    PK(p -> p.sendKey),
    PK_DIGEST(p -> p.sendKeyDigest),
    GENERATION(p -> p.sendGeneration),
    EXPIRATION(p -> p.sendExpiration),
    ;

    private final Function<DriverPolicy, Boolean> enabled;

    SpecialField(Function<DriverPolicy, Boolean> enabled) {
        this.enabled = enabled;
    }

    public boolean isEnabled(DriverPolicy driverPolicy) {
        return enabled.apply(driverPolicy);
    }

    public static Collection<SpecialField> specialFields(AerospikePolicyProvider policyProvider) {
        DriverPolicy driverPolicy = policyProvider.getDriverPolicy();
        boolean getPk = Stream.of(policyProvider.getQueryPolicy(), policyProvider.getBatchPolicy(), policyProvider.getScanPolicy()).anyMatch(p -> p.sendKey);
        Collection<SpecialField> configuredSpecialFields = Arrays.stream(SpecialField.values()).filter(f -> f.isEnabled(driverPolicy)).collect(toSet());
        if (getPk) {
            configuredSpecialFields.add(SpecialField.PK);
        }
        return configuredSpecialFields.isEmpty() ? emptySet() : EnumSet.copyOf(configuredSpecialFields);
    }

}
