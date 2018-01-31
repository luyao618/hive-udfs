package com.luis.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


/**
 * For a given grouping, return one array from the grouping. Good when you
 * want to associate each key with an arbitrary value, or for when you are
 * certain that every item in the grouping is the same.
 *
 * This replaces UDAFFirst.
 *
 * Note that this function does not guarantee that you will get the first
 * item, only that you will get one of the items.
 *
 * TODO: Make this work with Hive STRUCT types.
 */

/**
 * 不知道copyToStandardObject这个方法具体是什么 感觉像是对于传入的数据
 * 选一个 选的是第一个，因为是分布式，所以也可以认为是随机选了一个。
 * 这个可以用于 聚合函数获取结果里非聚合的维度 可以不适用max，min了，效率应该更高，但是要保证对于key 这个维度要是一样的数据，这样才能随机取
 */
@Description(name = "chooseone", value="_FUNC_(value) - Return an arbitrary value from the group")
public final class UDAFChooseOne extends AbstractGenericUDAFResolver {
  public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
    if (parameters.length != 1) {
      throw new UDFArgumentLengthException("Only one paramter expected, but you provided " + parameters.length);
    }
    return new Evaluator();
  }

  public static class Evaluator extends GenericUDAFEvaluator {
    ObjectInspector inputOI;

    /**
     * 初始化 每步都运行，确定输入和数据数据
     * 这个只有输入数据，数据数据就是入参（一个）
     * 没有输出数据，但是每一步会更改存放中间结果的类的值
     * @param m
     * @param parameters
     * @return
     * @throws HiveException
     */
    @Override
    public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
      super.init(m, parameters);
      inputOI = parameters[0];
      return ObjectInspectorUtils.getStandardObjectInspector(inputOI);
    }

    /**
     * 保存中间结果的静态类
     * 这个是object 所以什么类型都可以用这个函数
     */
    static class State implements AggregationBuffer {
      Object state = null;
    }
    
    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      return new State();
    }

    /**
     * 处理一行数据
     * @param agg
     * @param input
     * @throws HiveException
     */
    @Override
    public void iterate(AggregationBuffer agg, Object[] input) throws HiveException {
      State s = (State) agg;
      if (s.state == null) {
        s.state = ObjectInspectorUtils.copyToStandardObject(input[0], inputOI);
      }
    }

    /**
     * 将terminatePartial返回的部分聚合数据进行合并，需要使用到对应的OI
     * @param agg
     * @param partial
     * @throws HiveException
     */
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
      State s = (State) agg;
      if (s.state == null) {
        s.state = ObjectInspectorUtils.copyToStandardObject(partial, inputOI);
      }
    }
    
    @Override
    public void reset(AggregationBuffer agg) {
      State s = (State) agg;
      s.state = null;
    }

    /**
     * 结束，生成最终结果。
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      State s = (State) agg;
      return s.state;
    }

    /**
     * 返回部分聚合数据的持久化对象。
     * @param agg
     * @return
     * @throws HiveException
     */
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      State s = (State) agg;
      return s.state;
    }
  }
}
