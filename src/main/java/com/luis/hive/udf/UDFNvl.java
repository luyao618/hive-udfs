package com.luis.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * 这是《hive 编程指南》第十三章的一个例子，用于学习 继承GenericUDF 写hive udf
 *  1.继承GeneriUDF可以更好的处理null值操作，同事可以处理一些标砖的udf无法支持的编程操作
 *  2.这个函数的作用是 传入的值如果是null那么就发挥一个默认值
 *  3.测试示例 select nvl(null,5) as c1,nvl(1,5) as c2
 * Created by Luis on 2018/1/31.
 */
public class UDFNvl extends GenericUDF{

    //这个class 将帮助检测是否所有的可能性都完全一样 ObjectInspector。如果没有，那么我们需要将对象转换为相同的ObjectInspector
    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argumentOIs;
    /**
     * 这个函数会被输入的每个参数调用，并最终传入到一个ObjectInspector对象中。
     * 这个方法的目标是为了确定参数的返回类型
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        argumentOIs = arguments;
        //判断传入参数的个数 基本套路
        if(arguments.length != 2){
            throw new UDFArgumentLengthException("the operator 'NVL' accepts 2 arguments");
        }
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);//true 可以转变类型
        //// TODO: 2018/1/31 实际测试 两个不同的类型不抛异常。。。
        if(!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))){
            throw new UDFArgumentTypeException(2,"两个参数必须相同的类型！" + "第一个类型为：" + arguments[0].getTypeName() + "第二个类型为："+ arguments[1].getTypeName());
        }
        return returnOIResolver.get();//返回的是两个参数共有的类型
    }

    /**
     * evaluate输入的是DeferredObject，不是ObjectInspector，那个returnOIResolver对象就用于DeferredObject取值
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //这个逻辑很简单 就是return arguments[0]==null?arguments[1]:arguments[0]
        //但是为什么这么写 还是套路吧……
        Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(),argumentOIs[0]);
        if(retVal == null){
            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),argumentOIs[1]);
        }
        return retVal;
    }

    /**
     * 用于Hadoop task 内部 展示调试信息
     * @param children
     * @return
     */
    @Override
    public String getDisplayString(String[] children) {
        StringBuilder sb = new StringBuilder();
        sb.append("if ");
        sb.append("children[0]");
        sb.append("is null");
        sb.append("returns");
        sb.append("children[1]");
        return sb.toString();
    }
}
