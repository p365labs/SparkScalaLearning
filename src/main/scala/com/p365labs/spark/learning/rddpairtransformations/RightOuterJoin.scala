package com.p365labs.spark.learning.rddpairtransformations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class RightOuterJoin {
  def testRightOuterJoin(sc: SparkContext): Unit = {
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

    val result = elements.rightOuterJoin(other);

    println("");
    println("************************ PAIR RDD TRANSFORMATIONS RIGHTOUTERJOIN");
    println("Actions : elements is List((1,2),(3,4),(3,6))");
    println("Actions : other is List((3,9))");
    println("Actions : elements.rightOuterJoin(other)");
    for (res <- result) println("Actions : .rightOuterJoin(other) = " + res);
  }
}
