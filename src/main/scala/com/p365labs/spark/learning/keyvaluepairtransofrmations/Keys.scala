package com.p365labs.spark.learning.keyvaluepairtransofrmations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class Keys {
  def testKeys(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.keys;

    println("");
    println("************************ ACTION KEYS");
    println("Actions : elements.keys  where elements is List((1,2),(3,4),(3,6))");
    for (res <- result) println("Actions : .keys = " + res);

  }
}
