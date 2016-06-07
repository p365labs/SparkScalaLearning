package com.p365labs.spark.learning.keyvaluepairtransofrmations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class ReduceByKey {
  def testReduceByKey(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.reduceByKey((x, y) => x + y);

    println("");
    println("************************ ACTION REDUCEBYKEY");
    println("Actions : elements.reduceByKey((x, y) => x + y)  where elements is List((1,2),(3,4),(3,6))");
    for (res <- result) println("Actions : .reduceByKey((x, y) => x + y) = " + res);
  }
}
