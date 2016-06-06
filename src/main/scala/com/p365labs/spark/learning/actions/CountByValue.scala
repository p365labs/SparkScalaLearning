package com.p365labs.spark.learning.actions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class CountByValue {
  def testCountByValue(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,3));

    val result = elements.countByValue();
    println(result.getClass());
    println("Actions : countByValue = " + result);
  }
}
