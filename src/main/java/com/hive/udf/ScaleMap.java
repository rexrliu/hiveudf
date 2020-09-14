package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Scale all element in a maps by a factor.
 */
@Description(
        name = "scale_map",
        value = "_FUNC_(map, scalar) - scale map elements by a factor",
        extended = "Scale all element in a maps by a factor.")

public class ScaleMap extends GenericUDF {
    private MapObjectInspector mapInspector;
    private static final int ARG_COUNT = 2; // Number of arguments to this UDF

    @Override
    public String getDisplayString(String[] arg0) {
        return "scale_map()";
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments)
            throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentTypeException(arguments.length - 1,
                    "Exactly four arguments are expected.");
        }

        // return null if either the map or the scalar is null
        if ("void".equals(arguments[0].getTypeName()) || "void".equals(arguments[1].getTypeName())) {
            throw new UDFArgumentTypeException(0, "arguments cannot be null");
        }

        if (!arguments[0].getCategory().equals(ObjectInspector.Category.MAP)) { // Check if the argument is of category LIST
            throw new UDFArgumentTypeException(0,
                    "\"" + serdeConstants.MAP_TYPE_NAME + "\" "
                            + "expected by function scale_map as parameter 1, but "
                            + "\"" + arguments[0].getTypeName() + "\" "
                            + "is found");
        }

        switch (((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                break;
            default:
                throw new UDFArgumentTypeException(1,
                        "Only numeric type arguments are accepted for scale_map as parameter 2, but "
                                + arguments[1].getTypeName() + " is passed.");
        }

        mapInspector = (MapObjectInspector) arguments[0];

        ObjectInspector returnType = ObjectInspectorFactory.getStandardMapObjectInspector(
                mapInspector.getMapKeyObjectInspector(),
                PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
        return returnType;
    }

    @Override
    public Map<Object, Double> evaluate(DeferredObject[] arguments) throws HiveException {
        Object o_map = arguments[0].get();
        Object o_scalar = arguments[1].get();

        Map<Object, Double> outMap = new HashMap<Object, Double>();

        if (o_map == null || o_scalar == null) {  // check if argument is null
            return outMap;
        }

        Map inMap = mapInspector.getMap(o_map);
        double s = Double.parseDouble(o_scalar.toString());

        inMap.forEach((k, v) -> {
            if (v == null)
                outMap.put(k, null);
            else
                outMap.put(k, s * Double.parseDouble(v.toString()));
        });

        return outMap;
    }
}
