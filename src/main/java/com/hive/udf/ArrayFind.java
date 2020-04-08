package com.hive.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@Description(name = "array_index"
        , value = "_FUNC_(array, val) - returns the index of the input in the array."
        , extended = "Example:\n > select _FUNC_(array, val) from src;")

public class ArrayFind extends GenericUDF {
    private static final int ARG_COUNT = 2; // Number of arguments to this UDF

    private ObjectInspector valueOI;
    private ListObjectInspector arrayOI;
    private ObjectInspector arrayElementOI;
    private IntWritable result;

    public ArrayFind() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // Check if two arguments were passed
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentException("The function array_index accepts "
                    + ARG_COUNT + " arguments.");
        }

        if ("void".equals(arguments[0].getTypeName())) {  // check if input is null
            return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        // Check if ARRAY_IDX argument is of category LIST
        if (!arguments[0].getCategory().equals(Category.LIST)) {
            throw new UDFArgumentTypeException(0, "\"" + "LIST"
                    + "\" " + "expected at function array_index, but "
                    + "\"" + arguments[0].getTypeName() + "\" " + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[0];
        arrayElementOI = arrayOI.getListElementObjectInspector();
        valueOI = arguments[1];

        // Check if list element and value are of same type
        if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
            throw new UDFArgumentTypeException(1, "\"" + arrayElementOI.getTypeName() + "\""
                    + " expected at function array_index , but " + "\""
                    + valueOI.getTypeName() + "\"" + " is found");
        }

        // Check if the comparison is supported for this type
        if (!ObjectInspectorUtils.compareSupported(valueOI)) {
            throw new UDFArgumentException("The function array_index does not support comparison for "
                    + "\"" + valueOI.getTypeName() + "\"" + " types");
        }

        result = new IntWritable(-1);

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        result.set(-1);

        Object array = arguments[0].get();
        Object value = arguments[1].get();

        int arrayLength = arrayOI.getListLength(array);

        // Check if array is null or empty or value is null
        if (value == null || arrayLength <= 0) {
            return result;
        }

        // Compare the value to each element of array until a match is found
        for (int i = 0; i < arrayLength; i++) {
            Object v = arrayOI.getListElement(array, i);
            if (v != null) {
                if (ObjectInspectorUtils.compare(value, valueOI, v, arrayElementOI) == 0) {
                    result.set(i);
                    break;
                }
            }
        }

        return result;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_index_of(" + strings[0] + ", " + strings[1] + ")";
    }
}