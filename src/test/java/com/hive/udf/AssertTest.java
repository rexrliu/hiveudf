package com.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AssertTest {
    Assert udf = new Assert();
    String message = "Test Error!";

    @Test
    public void assert_ture() throws HiveException {
        assertEquals(null, udf.evaluate(true, message));
    }

    @Test(expected = HiveException.class)
    public void assert_false() throws HiveException {
        udf.evaluate(false, message);
    }
}
