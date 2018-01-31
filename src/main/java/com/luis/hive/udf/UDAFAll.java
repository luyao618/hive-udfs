package com.luis.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * Returns true if ALL of the column of booleans passed to the function are true.
 *
 * If any item is false, return false.
 * If there are no "false"s, but there are nulls, we don't know: Return null.
 * If all rows are true, return true.
 * If there are NO rows, both 0 are true and ALL are true. Return null.
 *
 * Semantically equivalent to:
 * NOT ANY(NOT column)
 */

/**
 * 继承UDAF类，然后实现UDAFEvaluator接口，这种写udaf的方式效率低，已经被弃用了
 */
public final class UDAFAll extends UDAF {

    public static class UDAFAllEvaluator implements UDAFEvaluator {

        Boolean result = null;
        Boolean any_rows_seen = false;

        //构造器
        public UDAFAllEvaluator() {
            super();
            init();
        }

        //初始化 私有变量初始值
        public void init() {
            result = null; // Return null for 0 rows
            any_rows_seen = false;
        }

        //map阶段 返回值为boolean
        public boolean iterate(Boolean ThisBool) {
            if (result != null && !result) {  // Finish state reached.
              ;
            } else if (ThisBool == null) {  // We can no longer return true.
                result = null; //如果任意一行为null 那么就返回null了
            } else if (!ThisBool) {  // Certainty! We have our answer.
                result = false;
            } else if (!any_rows_seen) {  // ThisBool must be true now
                result = true;  // Different initialization state for > 0 rows
            } else {
              ; // maintain current assumptions
            }
            any_rows_seen = true;
            return true;
        }

        // 类似于Combiner，不过只传给merge结果
        public Boolean terminatePartial() {
          return result;
        }

        //reduce阶段 返回值为boolean
        public boolean merge(Boolean soFar) {
            iterate(soFar);
            return true;
        }

        //返回聚合结果
        public Boolean terminate() {
            return result;
        }
    }
}
