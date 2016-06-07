package com.p365labs.spark.learning.keyvaluepairtransofrmations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class Map {
  def testMapRDDPairs(sc: SparkContext): Unit = {

    /**
      * Define element of a rating file with this form:
      *
      * Array(
      *         "userID, itemId, rating"
      * )
      */
    val elements = sc.parallelize(
      List(
        "1, 102, 1",
        "1, 103, 1",
        "1, 104, 1",
        "2, 104, 1",
        "3, 102, 1",
        "3, 104, 1",
        "4, 102, 1",
        "4, 104, 1",
        "5, 106, 1",
        "5, 102, 1",
        "6, 108, 1",
        "6, 102, 1",
        "7, 108, 1"
      )
    );

    /**
      * the data is only and array of single element if you want to create a pairRDD you need to add a new element
      * to map
      */
    val result = elements.map(x => (x.split(",")(0),x.split(",")(1) + "" + x.split(",")(2)));

    val resultOrdered = result.sortByKey(true);
    for (res <- resultOrdered) println(res);
  }
}
