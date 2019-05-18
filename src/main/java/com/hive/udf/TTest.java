package com.hive.udf;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;

import org.apache.commons.math3.distribution.TDistribution;


@Description(name = "t-test", value = "_FUNC_(y,x) - Run t-test between a set of number pairs")
public class TTest extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 2) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly two arguments are expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }

        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1,
                    "Only primitive type arguments are accepted but "
                            + parameters[1].getTypeName() + " is passed.");
        }

        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case TIMESTAMP:
            case DECIMAL:
                switch (((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case TIMESTAMP:
                    case DECIMAL:
                        return new TTestGenericEvaluator();
                    case STRING:
                    case BOOLEAN:
                    case DATE:
                    default:
                        throw new UDFArgumentTypeException(1,
                                "Only numeric type arguments are accepted but "
                                        + parameters[1].getTypeName() + " is passed.");
                }
            case STRING:
            case BOOLEAN:
            case DATE:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }

    /**
     *  Incremental calculation of average and (unbiased) sample variance:
     *   n : &lt;count&gt;
     *   mu_n = mu_(n-1) + (x_n - mu_(n-1)) / n : &lt;xavg&gt;
     *   var_n = (n - 2) * std_(n-1)^2 + (x_n - mu_n) * (x_n - mu_(n-1)) / (n - 1): &lt;unbiased estimate of std * n&gt;
     *
     *  Merge average and sample variance:
     *   mu_(A,B) = (n_A * mu_A + n_B * mu_B) / (n_A + n_B)
     *   var_(A,B) = [(n_A - 1) * std_A^2 + (n_B - 1) * std_B^2
     *     + (mx_A - mx_B)^2 *n_A * n_B / (n_A + n_B)] / (n_A + n_B - 1)
     *
     */
    public static class TTestGenericEvaluator extends GenericUDAFEvaluator {

        // For PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector xInputOI;
        private PrimitiveObjectInspector yInputOI;

        // For PARTIAL2 and FINAL
        private transient StructObjectInspector soi;
        private transient StructField xcountField;
        private transient StructField ycountField;
        private transient StructField xavgField;
        private transient StructField yavgField;
        private transient StructField xvarField;
        private transient StructField yvarField;

        private LongObjectInspector xcountFieldOI;
        private LongObjectInspector ycountFieldOI;
        private DoubleObjectInspector xavgFieldOI;
        private DoubleObjectInspector yavgFieldOI;
        private DoubleObjectInspector xvarFieldOI;
        private DoubleObjectInspector yvarFieldOI;

        // For PARTIAL1 and PARTIAL2
        private Object[] partialResult;

        // For FINAL and COMPLETE
        private Object[] result;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // init input
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                assert (parameters.length == 2);
                yInputOI = (PrimitiveObjectInspector) parameters[0];
                xInputOI = (PrimitiveObjectInspector) parameters[1];
            } else {
                assert (parameters.length == 1);
                soi = (StructObjectInspector) parameters[0];

                xcountField = soi.getStructFieldRef("xcount");
                ycountField = soi.getStructFieldRef("ycount");
                xavgField = soi.getStructFieldRef("xavg");
                yavgField = soi.getStructFieldRef("yavg");
                xvarField = soi.getStructFieldRef("xvar");
                yvarField = soi.getStructFieldRef("yvar");

                xcountFieldOI = (LongObjectInspector) xcountField.getFieldObjectInspector();
                ycountFieldOI = (LongObjectInspector) ycountField.getFieldObjectInspector();
                xavgFieldOI = (DoubleObjectInspector) xavgField.getFieldObjectInspector();
                yavgFieldOI = (DoubleObjectInspector) yavgField.getFieldObjectInspector();
                xvarFieldOI = (DoubleObjectInspector) xvarField.getFieldObjectInspector();
                yvarFieldOI = (DoubleObjectInspector) yvarField.getFieldObjectInspector();
            }

            // init output
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // The output of a partial aggregation is a struct containing
                // a long count, two double averages, two double variances,
                // and a double covariance.

                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();

                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableLongObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

                ArrayList<String> fname = new ArrayList<String>();
                fname.add("xcount");
                fname.add("ycount");
                fname.add("xavg");
                fname.add("yavg");
                fname.add("xvar");
                fname.add("yvar");

                partialResult = new Object[6];
                partialResult[0] = new LongWritable(0);
                partialResult[1] = new LongWritable(0);
                partialResult[2] = new DoubleWritable(0);
                partialResult[3] = new DoubleWritable(0);
                partialResult[4] = new DoubleWritable(0);
                partialResult[5] = new DoubleWritable(0);

                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);

            } else {
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);

                ArrayList<String> fname = new ArrayList<String>();
                fname.add("statistic");
                fname.add("pvalue");

                result = new Object[2];
                result[0] = new DoubleWritable(0);
                result[1] = new DoubleWritable(0);

                return ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi);
            }
        }

        @AggregationType(estimable = true)
        static class StdAgg extends AbstractAggregationBuffer {
            long xcount; // number of x elements
            long ycount; // number of y elements
            double xavg; // average of x elements
            double yavg; // average of y elements
            double xvar; // variance of x elements
            double yvar; // variance of y elements
            @Override
            public int estimate() { return JavaDataModel.PRIMITIVES2 * 6; }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            StdAgg result = new StdAgg();
            reset(result);
            return result;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            StdAgg myagg = (StdAgg) agg;
            myagg.xcount = 0;
            myagg.ycount = 0;
            myagg.xavg = 0;
            myagg.yavg = 0;
            myagg.xvar = 0;
            myagg.yvar = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 2);
            Object py = parameters[0];
            Object px = parameters[1];

            StdAgg myagg = (StdAgg) agg;
            if (px != null) {
                double v = PrimitiveObjectInspectorUtils.getDouble(px, xInputOI);
                myagg.xcount++;
                double delta = v - myagg.xavg;
                myagg.xavg += delta / myagg.xcount;
                if (myagg.xcount > 1) {
                    myagg.xvar = myagg.xvar * ((myagg.xcount - 2.0d) / (myagg.xcount - 1.0d))
                            + (v - myagg.xavg) * delta / (myagg.xcount - 1.0d);
                }
            }

            if (py != null) {
                double v = PrimitiveObjectInspectorUtils.getDouble(py, yInputOI);
                myagg.ycount++;
                double delta = v - myagg.yavg;
                myagg.yavg += delta / myagg.ycount;
                if (myagg.ycount > 1) {
                    myagg.yvar = myagg.yvar * ((myagg.ycount - 2.0d) / (myagg.ycount - 1.0d))
                            + (v - myagg.yavg) * delta / (myagg.ycount - 1.0d);
                }
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            StdAgg myagg = (StdAgg) agg;
            ((LongWritable) partialResult[0]).set(myagg.xcount);
            ((LongWritable) partialResult[1]).set(myagg.ycount);
            ((DoubleWritable) partialResult[2]).set(myagg.xavg);
            ((DoubleWritable) partialResult[3]).set(myagg.yavg);
            ((DoubleWritable) partialResult[4]).set(myagg.xvar);
            ((DoubleWritable) partialResult[5]).set(myagg.yvar);
            return partialResult;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                StdAgg myagg = (StdAgg) agg;

                Object partialXCount = soi.getStructFieldData(partial, xcountField);
                Object partialYCount = soi.getStructFieldData(partial, ycountField);
                Object partialXAvg = soi.getStructFieldData(partial, xavgField);
                Object partialYAvg = soi.getStructFieldData(partial, yavgField);
                Object partialXVar = soi.getStructFieldData(partial, xvarField);
                Object partialYVar = soi.getStructFieldData(partial, yvarField);

                long nA = myagg.xcount;
                long nB = xcountFieldOI.get(partialXCount);

                if (nA == 0) {
                    // Just copy the information since there is nothing so far
                    myagg.xcount = xcountFieldOI.get(partialXCount);
                    myagg.xavg = xavgFieldOI.get(partialXAvg);
                    myagg.xvar = xvarFieldOI.get(partialXVar);
                }

                if (nA != 0 && nB != 0) {
                    // Merge the two partials
                    double xavgA = myagg.xavg;
                    double xvarA = myagg.xvar;
                    double xavgB = xavgFieldOI.get(partialXAvg);
                    double xvarB = xvarFieldOI.get(partialXVar);

                    myagg.xcount += nB;
                    myagg.xavg = xavgA * (nA / (double) myagg.xcount)
                            + xavgB * (nB / (double) myagg.xcount);
                    myagg.xvar = xvarA * ((nA - 1.0d) / (myagg.xcount - 1.0d))
                            + xvarB * ((nB - 1.0d) / (myagg.xcount - 1.0d))
                            + (xavgA - xavgB) * (xavgA - xavgB)
                            * (nA / (double) (nA + nB)) * (nB / (myagg.xcount - 1.0d));
                }

                nA = myagg.ycount;
                nB = ycountFieldOI.get(partialYCount);

                if (nA == 0) {
                    // Just copy the information since there is nothing so far
                    myagg.ycount = xcountFieldOI.get(partialYCount);
                    myagg.yavg = xavgFieldOI.get(partialYAvg);
                    myagg.yvar = xvarFieldOI.get(partialYVar);
                }

                if (nA != 0 && nB != 0) {
                    // Merge the two partials
                    double yavgA = myagg.yavg;
                    double yvarA = myagg.yvar;
                    double yavgB = yavgFieldOI.get(partialYAvg);
                    double yvarB = yvarFieldOI.get(partialYVar);

                    myagg.ycount += nB;
                    myagg.yavg = yavgA * (nA / (double) myagg.ycount)
                            + yavgB * (nB / (double) myagg.ycount);
                    myagg.yvar = yvarA * ((nA - 1.0d) / (myagg.ycount - 1.0d))
                            + yvarB * ((nB - 1.0d) / (myagg.ycount - 1.0d))
                            + (yavgA - yavgB) * (yavgA - yavgB)
                            * (nA / (double) (nA + nB)) * (nB  / (myagg.ycount - 1.0d));
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            StdAgg myagg = (StdAgg) agg;

            if (myagg.xcount == 0 || myagg.ycount == 0 || myagg.xvar == 0.0d || myagg.yvar == 0.0d) {
                return null;
            } else {
                double s = myagg.xvar / myagg.xcount + myagg.yvar / myagg.ycount;
                double t = java.lang.Math.abs((myagg.xavg - myagg.yavg) / java.lang.Math.sqrt(s));
                double df = s * s / ((myagg.xvar / myagg.xcount)
                        * (myagg.xvar / myagg.xcount) / (myagg.xcount - 1.0d)
                        + (myagg.yvar / myagg.ycount) * (myagg.yvar / myagg.ycount) / (myagg.ycount - 1.0d));

                result = new Object[2];
                result[0] = new DoubleWritable(t);
                result[1] = new DoubleWritable(2 * (new TDistribution(df)).cumulativeProbability(-t));

                return result;
            }
        }

        public void setResult(Object[] result) {
            this.result = result;
        }

        public Object[] getResult() {
            return result;
        }
    }
}