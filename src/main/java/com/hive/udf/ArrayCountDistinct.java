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

import java.util.ArrayList;

//import org.apache.hadoop.hive.serde2.io.;

@Description(name = "array_countd"
        , value = "_FUNC_(array) - returns the number of distinct values in an input array."
        , extended = "Example:\n > select _FUNC_(array) from src;")
public class ArrayCountDistinct extends GenericUDF {
    private static final int ARG_COUNT = 1; // Number of arguments to this UDF
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    public ArrayCountDistinct() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {  // Check if only one argument was passed
            throw new UDFArgumentLengthException(
                    "The function array_countd(array) takes exactly " + ARG_COUNT + " arguments.");
        }

        if ("void".equals(arguments[0].getTypeName())) {  // check if input is null
            return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        if (!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) { // Check if the argument is of category LIST
            throw new UDFArgumentTypeException(0,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected by function array_countd, but "
                            + "\"" + arguments[0].getTypeName() + "\" "
                            + "is found");
        }

        arrayOI = (ListObjectInspector) arguments[0];
        arrayElementOI = arrayOI.getListElementObjectInspector();

        // Check if the comparison is supported for this type
        if (!ObjectInspectorUtils.compareSupported(arrayElementOI)) {
            throw new UDFArgumentException("The function array_countd"
                    + " does not support comparison for "
                    + "\"" + arrayElementOI.getTypeName() + "\""
                    + " types");
        }

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[0].get();
        if (array == null) {  // Check if array is null
            return null;
        }

        ArrayList<Object> uniq = new ArrayList<Object>();
        for (int i = 0; i < arrayOI.getListLength(array); i++) {
            Object v = arrayOI.getListElement(array, i);
            if (v != null) {
                boolean found = false;
                for (Object e : uniq) {
                    if (v.equals(e)) {
                        found = true;
                        break;
                    }
                }

                if (!found) {
                    uniq.add(v);
                }
            }
        }

        return new IntWritable(uniq.size());
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_countd(" + strings[0] + ")";
    }
}