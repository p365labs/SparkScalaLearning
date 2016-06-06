package com.p365labs.spark.learning.actions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class TakeSample {
  def testTakeSample(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10));
    val result = elements.takeSample(false, 1);

    println("Actions : .takeSample(false, 1) = " + result);
  }
}
