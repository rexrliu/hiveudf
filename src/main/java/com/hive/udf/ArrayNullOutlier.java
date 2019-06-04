package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

@Description(name = "array_avg"
        , value = "_FUNC_(array) - replace outliers (beyond [lower, upper]) with null in an input array."
        , extended = "Example:\n > select _FUNC_(array) from src;")
public class ArrayNullOutlier extends GenericUDF {
    private static final int ARG_COUNT = 3; // Number of arguments to this UDF
    private transient ListObjectInspector arrayOI;
    private transient ObjectInspector arrayElementOI;

    public ArrayNullOutlier() {
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {  // Check if the required arguments were passed
            throw new UDFArgumentLengthException(
                    "The function array_nullout(array) takes exactly " + ARG_COUNT + " arguments.");
        }

        if ("void".equals(arguments[0].getTypeName()) || "void".equals(arguments[1].getTypeName())) {  // check if input is null
            return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        if (!arguments[0].getCategory().equals(ObjectInspector.Category.LIST)) { // Check if the argument is of category LIST
            throw new UDFArgumentTypeException(0,
                    "\"" + org.apache.hadoop.hive.serde.serdeConstants.LIST_TYPE_NAME + "\" "
                            + "expected by function array_nullout as parameter 1, but "
                            + "\"" + arguments[0].getTypeName() + "\" "
                            + "is found");
        }

        if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "Only primitive type arguments are accepted as parameter 2 but "
                            + arguments[1].getTypeName() + " is passed.");
        }

        if (arguments[2].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "Only primitive type arguments are accepted as parameter 3 but "
                            + arguments[2].getTypeName() + " is passed.");
        }

        arrayOI = (ListObjectInspector) arguments[0];
        arrayElementOI = arrayOI.getListElementObjectInspector();

        // Check if the comparison is supported for this type
        if (!ObjectInspectorUtils.compareSupported(arrayElementOI)) {
            throw new UDFArgumentException("The function array_unique"
                    + " does not support comparison for "
                    + "\"" + arrayElementOI.getTypeName() + "\""
                    + " types");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(arrayElementOI);
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

        Object lower = arguments[1].get();
        Object upper = arguments[2].get();

        ArrayList<Object> res = new ArrayList<Object>();
        for (int i = 0; i < arrayLength; i++) {
            Object v = arrayOI.getListElement(array, i);
            if (ObjectInspectorUtils.compare(upper, this.arrayElementOI, v, this.arrayElementOI) > 0 &&
                    ObjectInspectorUtils.compare(lower, this.arrayElementOI, v, this.arrayElementOI) < 0) {
                res.add(v);
            } else {
                res.add(null);
            }
        }

        return res;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_nullout(" + strings[0] + ", " + strings[1] + ", " + strings[2] + ")";
    }
}