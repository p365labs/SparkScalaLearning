package com.p365labs.spark.learning.keyvaluepairtransofrmations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class MapValues {
  def testMapValues(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.mapValues(x => x + 1);

    println("");
    println("************************ ACTION MAPVALUES");
    println("Actions : elements.mapValues(x => x + 1)  where elements is List((1,2),(3,4),(3,6))");
    for (res <- result) println("Actions : .mapValues(x => x + 1) = " + res);

  }
}
