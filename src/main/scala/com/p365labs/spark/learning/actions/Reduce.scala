package com.p365labs.spark.learning.actions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class Reduce {
  def testReduce(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,3));

    val result = elements.reduce((x, y) => x + y);

    println("The reduce result is : " + result);
  }
}
