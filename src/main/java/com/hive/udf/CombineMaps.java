package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.HashMap;
import java.util.Map;


/**
 * Aggregate all maps into a single map. If there are multiple value for same
 * key, result is the sum of those values.
 * Because the mappers must keep all of the data in memory, if your data is
 * non-trivially large you should set hive.map.aggr=false to ensure that
 * Combine_Map is only executed in the reduce phase.
 */
@Description(
        name = "combine_map",
        value = "_FUNC_(map) - aggregate given maps into a single map",
        extended = "Aggregate maps, returns as a HashMap.")

public class CombineMaps extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        return new Evaluator();
    }

    public static class State implements AggregationBuffer {
        HashMap<Object, Double> map = new HashMap<Object, Double>();
    }

    public static class Evaluator extends GenericUDAFEvaluator {
        ObjectInspector inputOI;
        MapObjectInspector internalMergeOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.COMPLETE || m == Mode.PARTIAL1) {
                inputOI = (MapObjectInspector) parameters[0];
            } else {
                internalMergeOI = (MapObjectInspector) parameters[0];
            }

            ObjectInspector returnType = ObjectInspectorFactory.getStandardMapObjectInspector(
                    ((MapObjectInspector) parameters[0]).getMapKeyObjectInspector(),
                    PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
            return returnType;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new State();
        }

        private void reduce(Map<Object, Double> ma, Map<Object, Object> mb) {
            mb.forEach((k, v) -> {
                if (v == null) {
                    if(! ma.containsKey(k))
                        ma.put(k, null);
                } else {

                    if(ma.containsKey(k))
                        ma.put(k, ma.get(k) + Double.parseDouble(v.toString()));
                    else
                        ma.put(k, Double.parseDouble(v.toString()));
                }
            });
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
            if (input[0] != null) {
                State state = (State) agg;
                reduce(state.map, (Map<Object, Object>) ObjectInspectorUtils.copyToStandardObject(input[0], inputOI));
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                State state = (State) agg;
                Map<Object, Object> pset = (Map<Object, Object>) ObjectInspectorUtils.copyToStandardObject(partial, internalMergeOI);
                reduce(state.map, pset);
            }
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((State) agg).map.clear();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            return ((State) agg).map;
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return ((State) agg).map;
        }
    }
}