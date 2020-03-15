package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import java.lang.String;
import org.apache.hadoop.io.Text;

@Description(name = "hex_encrypt"
        , value = "_FUNC_(hex_string, key) - returns the bit xor for the two hex strings."
        , extended = "Example:\n > select _FUNC_(string, string) from src;")
public class HexEncrypt extends GenericUDF {
    private static final int ARG_COUNT = 2;  // Number of arguments to this UDF
    private static final String HEX_CHARS = "0123456789abcdef";  //

    PrimitiveObjectInspector outputOI;

    public HexEncrypt() {}

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != ARG_COUNT) {
            throw new UDFArgumentTypeException(arguments.length - 1,
                    "Exactly two arguments are expected.");
        }

        if ("void".equals(arguments[0].getTypeName())) {  // check if input is null
            return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        if ("void".equals(arguments[1].getTypeName())) {  // check if input is null
            throw new UDFArgumentTypeException(1, "Key cannot be null.");
            //return PrimitiveObjectInspectorFactory.javaVoidObjectInspector;
        }

        if (((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()
                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "\"" + serdeConstants.STRING_TYPE_NAME + "\" "
                    + "expected by function hex_encrpyt as input string, but "
                    + "\"" + arguments[0].getTypeName() + "\" is found");
        }

        if (((PrimitiveObjectInspector) arguments[1]).getPrimitiveCategory()
                != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(1, "\"" + serdeConstants.STRING_TYPE_NAME + "\" "
                    + "expected by function hex_encrpyt as key, but "
                    + "\"" + arguments[1].getTypeName() + "\" is found");
        }

        outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;

        return outputOI;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o_val = arguments[0].get();
        if (o_val == null) {  // Check if array is null
            return null;
        }

        Object o_key = arguments[1].get();

        String val = o_val.toString().toLowerCase();
        String key = o_key.toString().toLowerCase();

        int nk = key.length();
        int nv = val.length();
        if (nk <= 0) {
            throw new HiveException("Key cannot be empty.");
        }

        char[] res = new char[nv];
        int k = 0;
        for (int i = 0; i < nv; i++) {
            char c = val.charAt(i);
            int bv = HEX_CHARS.indexOf(c);
            if (bv < 0) {
                res[i] = c;
            } else {
                char s = key.charAt(k % nk);
                int bk = HEX_CHARS.indexOf(s);
                if (bk < 0) {
                    throw new HiveException(String.valueOf(s) + " is not a valid hex char.");
                }

                res[i] =  Integer.toHexString(bv ^ bk).charAt(0);

                k++;
            }
        }

        return new Text(String.valueOf(res));
    }

    @Override
    public String getDisplayString(String[] strings) {
        assert (strings.length == ARG_COUNT);
        return "hex_encrypt(" + strings[0] + ")";
    }
}