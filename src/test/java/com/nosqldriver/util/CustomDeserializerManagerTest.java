package com.nosqldriver.util;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomDeserializerManagerTest {
    private static final String[] selectors = {"test:table:field", "test:table:*", "test:*:*", "test:*:field", "*:*:field", "*:table:*", "*:*:*"};

    @Test
    void empty() {
        CustomDeserializerManager cdm = new CustomDeserializerManager();
        for (Class<?> clazz : new Class[] {byte[].class, String.class, Long.class}) {
            assertFalse(cdm.getDeserializer("test:table:field", clazz).isPresent());
        }
    }

    @Test
    void oneDeserializer() {
        for (String selector : selectors) {
            CustomDeserializerManager cdm = new CustomDeserializerManager();
            cdm.addDeserializer(selector, ByteArrayToStringDeserialzier.class.getName());
            Optional<? extends Function<byte[], ?>> actual = cdm.getDeserializer("test:table:field", byte[].class);
            assertFalse(cdm.getDeserializer("test:table:field", Long.class).isPresent());
            assertTrue(actual.isPresent(), selector);
            assertEquals(ByteArrayToStringDeserialzier.class, actual.get().getClass(), selector);
        }
    }

    @Test
    void twoDeserializersDifferentTypesSameField() {
        for (String selector : selectors) {
            CustomDeserializerManager cdm = new CustomDeserializerManager();
            cdm.addDeserializer(selector, ByteArrayToStringDeserialzier.class.getName());
            cdm.addDeserializer(selector, LongToStringDeserialzier.class.getName());

            Optional<? extends Function<byte[], ?>> actualBytes = cdm.getDeserializer("test:table:field", byte[].class);
            assertTrue(actualBytes.isPresent(), selector);
            assertEquals(ByteArrayToStringDeserialzier.class, actualBytes.get().getClass(), selector);

            Optional<? extends Function<Long, ?>> actualLong = cdm.getDeserializer("test:table:field", Long.class);
            assertTrue(actualLong.isPresent(), selector);
            assertEquals(LongToStringDeserialzier.class, actualLong.get().getClass(), selector);
        }
    }


    @Test
    void twoDeserializersDifferentFieldsSameType() {
        CustomDeserializerManager cdm = new CustomDeserializerManager();
        cdm.addDeserializer("test:table:one", ByteArrayToStringDeserialzier.class.getName());
        cdm.addDeserializer("test:table:two", ByteArrayToStringDeserialzier.class.getName());

        Optional<? extends Function<byte[], ?>> actual1 = cdm.getDeserializer("test:table:one", byte[].class);
        assertTrue(actual1.isPresent());
        assertEquals(ByteArrayToStringDeserialzier.class, actual1.get().getClass());

        Optional<? extends Function<byte[], ?>> actual2 = cdm.getDeserializer("test:table:two", byte[].class);
        assertTrue(actual2.isPresent());
        assertEquals(ByteArrayToStringDeserialzier.class, actual2.get().getClass());

        assertFalse(cdm.getDeserializer("test:table:one", Long.class).isPresent());
        assertFalse(cdm.getDeserializer("test:table:two", Long.class).isPresent());

        assertFalse(cdm.getDeserializer("test:table:three", byte[].class).isPresent());
        assertFalse(cdm.getDeserializer("test:table:four", byte[].class).isPresent());
    }


    public static class ByteArrayToStringDeserialzier implements Function<byte[], String> {
        @Override
        public String apply(byte[] bytes) {
            return new String(bytes);
        }
    }

    public static class LongToStringDeserialzier implements Function<Long, String> {
        @Override
        public String apply(Long l) {
            return l.toString();
        }
    }
}