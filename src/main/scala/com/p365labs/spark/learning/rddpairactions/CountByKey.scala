package com.p365labs.spark.learning.rddpairactions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class CountByKey {
  def testCountByKey(sc: SparkContext) {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.countByKey();

    println("");
    println("************************ PAIR RDD ACTIONS COUNTBYKEY");
    println("Actions : elements is List((1,2),(3,4),(3,6))");
    println("Actions : elements.countByKey()");
    for (res <- result) println("Actions : .countByKey() = " + res);
  }
}
