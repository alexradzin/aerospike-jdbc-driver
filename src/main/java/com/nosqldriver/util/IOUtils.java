package com.nosqldriver.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;

public class IOUtils {

    public static byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16 * 1024];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        return buffer.toByteArray();
    }


    public static String toString(Reader reader) throws IOException {
        StringWriter writer = new StringWriter();
        int nRead;
        char[] data = new char[16 * 1024];
        while ((nRead = reader.read(data, 0, data.length)) != -1) {
            writer.write(data, 0, nRead);
        }
        return writer.toString();
    }

    public static Object deserialize(Object any) {
        try {
            if (any instanceof byte[]) {
                byte[] bytes = (byte[])any;
                DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
                return Class.forName("com.nosqldriver.aerospike.sql.PreparedStatementWithComplexTypesTest$MyNotSerializableClass").getConstructor(int.class,String.class).newInstance(dis.readInt(), dis.readUTF());
            }
            return any;
        } catch (Exception e) {
            return new Object(); //??? this is a patch relevant in phase of metadata discovery.
        }
    }

}
