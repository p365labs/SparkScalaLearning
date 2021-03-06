package com.p365labs.spark.learning.keyvaluepairtransofrmations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class SortByKey {
  def testSortByKey(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val result = elements.sortByKey(true);

    println("");
    println("************************ ACTION SORTBYKEY");
    println("Actions : elements.sortByKey(true)  where elements is List((1,2),(3,4),(3,6))");
    for (res <- result) println("Actions : .sortByKey(true) = " + res);
  }
}
