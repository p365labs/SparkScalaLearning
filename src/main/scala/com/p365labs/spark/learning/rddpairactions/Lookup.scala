package com.p365labs.spark.learning.rddpairactions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class Lookup {
  def testLookUp(sc: SparkContext) {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.lookup(3);

    println("");
    println("************************ PAIR RDD ACTIONS LOOKUP");
    println("Actions : elements is List((1,2),(3,4),(3,6))");
    println("Actions : elements.lookup(3)");
    for (res <- result) println("Actions : .lookup(3) = " + res);
  }
}
