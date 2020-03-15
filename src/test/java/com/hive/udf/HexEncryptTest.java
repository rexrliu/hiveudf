package com.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HexEncryptTest {
    static final Text res = new Text("838f3bcd-6cc7-01d4-1166-f937ba1e8c24");
    static final String val = "4fca261c-4cbc-48af-973e-21328334d1d1";
    static final String key = "cc451dd1207b497b8658d805392a5df5";

    HexEncrypt udf;

    @Before
    public void setUp() throws Exception {
        udf = new HexEncrypt();
    }

    @Test
    public void evaluate() throws HiveException {
        DeferredObject [] args = new DeferredObject[2];
        args[0] = new DeferredJavaObject(val);
        args[1] = new DeferredJavaObject(key);

        assertEquals(res, udf.evaluate(args));
    }
}
