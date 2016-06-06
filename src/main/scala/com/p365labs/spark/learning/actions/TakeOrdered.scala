package com.p365labs.spark.learning.actions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class TakeOrdered {
  def testTakeOrdered(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10));
    val result = elements.takeOrdered(2);
    println("Actions : .takeOrdered(2) = " + result);
  }
}
