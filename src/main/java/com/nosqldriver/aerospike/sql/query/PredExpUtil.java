package com.nosqldriver.aerospike.sql.query;

import com.aerospike.client.query.PredExp;
import com.nosqldriver.VisibleForPackage;

import java.lang.reflect.Field;

/**
 * {@link PredExp} is internal class of Aerospike Client. This class is (ab)used in this JDBC driver for any filtering conditions
 * including conditions based on Primary Key. So, we have to extract private fields {@code type} and {@code value} from {@link PredExp}.
 * This makes us to use reflection.
 * TODO: Generally this is a bad solution better solution is to create some kind of predicate classes on the driver layer and transform them to {@link PredExp} or PK just before using Aerospike API
 */
@VisibleForPackage
class PredExpUtil {
    @VisibleForPackage
    static int extractType(PredExp predExp) {
        try {
            return (int)makeAccessible(predExp.getClass().getDeclaredField("type")).get(predExp);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            return -1;
        }
    }

    /**
     PredExp
     private static final int INTEGER_BIN = 100;
     private static final int STRING_BIN = 101;
     private static final int GEOJSON_BIN = 102;
     private static final int LIST_BIN = 103;
     private static final int MAP_BIN = 104;
     *
     * @param type
     * @return
     */
    @VisibleForPackage
    static boolean isBin(int type) {
        return type >= 100 && type <= 104;
    }


    /*
	private static final int INTEGER_VALUE = 10;
	private static final int STRING_VALUE = 11;
	private static final int GEOJSON_VALUE = 12;
     */
    @VisibleForPackage
    static boolean isValue(int type) {
        return type >= 10 && type <= 12;
    }

    @VisibleForPackage
    @SuppressWarnings("unchecked")
    static <T> T getValue(PredExp predExp) {
        try {
            return (T)makeAccessible(predExp.getClass().getDeclaredField("value")).get(predExp);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Field makeAccessible(Field field) {
        field.setAccessible(true);
        return field;
    }
}
