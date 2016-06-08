package com.p365labs.spark.learning.rddpairactions

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class CollectAsMap {
  def testCollectAsMap(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.collectAsMap();

    println("");
    println("************************ PAIR RDD ACTIONS COLLECTASMAP");
    println("Actions : elements is List((1,2),(3,4),(3,6))");
    println("Actions : elements.collectAsMap()");
    for (res <- result) println("Actions : .collectAsMap() = " + res);
  }
}
