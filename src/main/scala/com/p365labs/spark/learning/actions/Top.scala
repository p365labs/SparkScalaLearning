package com.p365labs.spark.learning.actions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class Top {
  def testTop(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10));
    val result = elements.top(3);

    println("");
    println("************************ ACTION TOP");
    println("Actions : elements.top(3)  where elements is List(1,2,3,4,5,6,7,8,9,10)");
    for (res <- result) println("Actions : .top(3) = " + res);
  }
}
