package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(name = "array_count"
        , value = "_FUNC_(array) - returns the non-null count of an input array."
        , extended = "Example:\n > select _FUNC_(array) from src;")
public class ArrayCount extends GenericUDF {
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    public ArrayCount() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {  // Check if only one argument was passed
            throw new UDFArgumentLengthException(
                    "The function array_count(array) takes exactly " + ARG_COUNT + " arguments.");
        }

        if ("void".equals(arguments[0].getTypeName())) {  // check if input is null
            return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        if (!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) { // Check if the argument is of category LIST
            throw new UDFArgumentTypeException(0,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected by function array_count, but "
                            + "\"" + arguments[0].getTypeName() + "\" "
                            + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[0];
        arrayElementOI = arrayOI.getListElementObjectInspector();

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[0].get();
        if (array == null) {  // Check if array is null
            return null;
        }

        int count = 0;
        for (int i = 0; i < arrayOI.getListLength(array); i++) {
            if (arrayOI.getListElement(array, i) != null)
                count++;
        }

        return new IntWritable(count);
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_count(" + strings[0] + ")";
    }
}