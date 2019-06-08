package com.hive.udf;

import java.util.List;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

@Description(name = "array_index"
        , value = "_FUNC_(array) - returns the index of the input in the array."
        , extended = "Example:\n > select _FUNC_(array) from src;")

public class ArraySet extends GenericUDF {
    private static final int ARG_COUNT = 3; // Number of arguments to this UDF

    private ListObjectInspector arrayOI;
    private ObjectInspector arrayElementOI;
    private ObjectInspector valueOI;
    private ObjectInspector IndexOI;

    public ArraySet() {
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
        IndexOI = arguments[1];
        valueOI = arguments[2];

        if (!((PrimitiveTypeInfo) IndexOI).getPrimitiveCategory()
                .equals(PrimitiveObjectInspector.PrimitiveCategory.INT)) {
            throw new UDFArgumentTypeException(1,
                    "Only int type arguments are accepted as index but "
                            + IndexOI.getTypeName() + " is passed.");
        }

        // Check if list element and value are of same type
        if (!ObjectInspectorUtils.compareTypes(arrayElementOI, valueOI)) {
            throw new UDFArgumentTypeException(1, "\"" + arrayElementOI.getTypeName() + "\""
                    + " expected at function array_index , but " + "\""
                    + valueOI.getTypeName() + "\"" + " is found");
        }

        return ObjectInspectorFactory.getStandardListObjectInspector(arrayElementOI);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object array = arguments[0].get();
        Object index = arguments[1].get();
        Object value = arguments[2].get();

        if (array == null) {  // Check if array is null
            return null;
        }

        int arrayLength = arrayOI.getListLength(array);
        if (arrayLength <= 0) {  // Check if array is empty
            return null;
        }

        int i = PrimitiveObjectInspectorUtils.getInt(index, (PrimitiveObjectInspector) IndexOI);
        if (i < 0 || i >= arrayLength) {
            throw new HiveException("The given index is out of the array boundary");
        }

        ((List<Object>) arrayOI).set(i, value);

        return arrayOI;
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "array_index_of(" + strings[0] + ", " + strings[1] + ")";
    }
}