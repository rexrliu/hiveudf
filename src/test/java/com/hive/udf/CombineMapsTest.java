package com.hive.udf;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class CombineMapsTest {
    @Test
    public void test() throws HiveException {
        CombineMaps.State agg = new CombineMaps.State();
        CombineMaps.Evaluator udf = new CombineMaps.Evaluator();

        ObjectInspector[] initArgs = new ObjectInspector[1];

        initArgs[0] = ObjectInspectorFactory.getStandardMapObjectInspector(
                PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);

        udf.init(GenericUDAFEvaluator.Mode.COMPLETE, initArgs);

        Map<String, Double>[] input = new Map[1];

        input[0] = new HashMap<String, Double>() {{
            put("a", Double.valueOf(6));
            put("b", Double.valueOf(8));
        }};

        udf.iterate(agg, input);

        input[0] = new HashMap<String, Double>() {{
            put("a", Double.valueOf(10));
            put("c", Double.valueOf(12));
        }};

        udf.iterate(agg, input);

        Map<Object, Double> res = (Map<Object, Double>) udf.terminate(agg);
        assertEquals(16.0d, res.get("a").doubleValue(), 1E-6);
    }
}
