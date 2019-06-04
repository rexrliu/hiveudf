package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

@Description(name = "array_avg"
        , value = "_FUNC_(array) - returns the average of an input array."
        , extended = "Example:\n > select _FUNC_(array) from src;")
public class ArrayAvg extends GenericUDF {
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    public ArrayAvg() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {  // Check if the required number of arguments were passed
            throw new UDFArgumentLengthException(
                    "The function array_avg(array) takes exactly " + ARG_COUNT + " arguments.");
        }

        if ("void".equals(arguments[0].getTypeName())) {  // check if input is null
            return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        if (!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) { // Check if the argument is of category LIST
            throw new UDFArgumentTypeException(0,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected by function array_avg, but "
                            + "\"" + arguments[0].getTypeName() + "\" "
                            + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[0];
        arrayElementOI = arrayOI.getListElementObjectInspector();

        String elementType = arrayElementOI.getTypeName().toUpperCase();
        if (elementType.contains("TINYINT") || elementType.contains("SMALLINT") ||
                elementType.contains("INT") || elementType.contains("BIGINT") ||
                elementType.contains("FLOAT") || elementType.contains("DOUBLE") ||
                elementType.contains("DECIMAL")) {
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        } else {
            throw new UDFArgumentTypeException(0,
                    "Only numeric type arguments are accepted but "
                            + arrayElementOI.getTypeName() + " is passed.");
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[0].get();
        if (array == null) {  // Check if array is null
            return null;
        }

        int arrayLength = arrayOI.getListLength(array);
        if (arrayLength <= 0) {  // Check if array is empty
            return null;
        }

        double sum = 0;
        int n = 0;
        for (int i = 0; i < arrayLength; i++) {
            Object v = arrayOI.getListElement(array, i);
            if (v != null) {
                n++;
                try {
                    sum += Double.parseDouble(v.toString());
                } catch (NumberFormatException formatExc) {
                    throw new UDFArgumentTypeException(0,
                            "Only numeric type arguments are accepted but "
                                    + arrayElementOI.getTypeName() + " was passed as parameter 1.");
                }
            }
        }

        return new DoubleWritable(sum / n);
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_avg(" + strings[0] + ")";
    }
}