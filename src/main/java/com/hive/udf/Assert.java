package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.metadata.HiveException;

@Description(name = "assert",
        value = "_FUNC_(condition, message) " +
                " - Returns null if the condition is TRUE, otherwise throws a Hive error with the given message")
public class Assert extends UDF {
    public String evaluate(boolean condition, String message) throws HiveException {
        if(!condition) {
            throw new HiveException(message);
        }

        return null;
    }
}
