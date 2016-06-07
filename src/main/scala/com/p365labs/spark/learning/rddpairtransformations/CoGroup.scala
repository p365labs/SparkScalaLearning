package com.p365labs.spark.learning.rddpairtransformations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class CoGroup {
  def testCoGroup(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val other = sc.parallelize(List(
      (3, 9)
    )
    );

    val result = elements.cogroup(other);

    println("");
    println("************************ PAIR RDD TRANSFORMATIONS COGROUP");
    println("Actions : elements is List((1,2),(3,4),(3,6))");
    println("Actions : other is List((3,9))");
    println("Actions : elements.cogroup(other)");
    for (res <- result) println("Actions : .cogroup(other) = " + res);
  }
}
