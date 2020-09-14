package com.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ScaleMapTest {
    @Test
    public void test() throws HiveException {
        ScaleMap udf = new ScaleMap();


        ObjectInspector[] initArgs = new ObjectInspector[2];

        initArgs[0] = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

        initArgs[1] = PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;

        udf.initialize(initArgs);

        DeferredObject[] args = new DeferredObject[2];

        Map<String, Double> obj = new HashMap<String, Double>() {{
            put("a", Double.valueOf(6));
            put("b", Double.valueOf(8));
        }};

        args[0] = new DeferredJavaObject(obj);
        args[1] = new DeferredJavaObject(Double.valueOf(2));


        Map<Object, Double> res = udf.evaluate(args);
        assertEquals(16.0d, res.get("b").doubleValue(), 1E-6);
    }
}
