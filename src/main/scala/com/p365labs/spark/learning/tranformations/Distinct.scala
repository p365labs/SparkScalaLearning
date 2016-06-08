package com.p365labs.spark.learning.tranformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by federicopanini on 06/06/16.
  */
class Distinct {
  def testDistinct(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,4,5,6,7,5,4,4,4,4,3,2,2,2,2,2,2,1));

    val valuesDistinct = elements.distinct();

    println("");
    println("************************ TRANSFORMATION DISTINCT");
    println("Actions : values.distinct()  where elements is List(\"coca cola\", \"coffee\", \"Sprite\", \"Redbull\")");
    println("TotalValues (Distinct operation): " + elements.count());
    for (res <- valuesDistinct) println("Actions : .top(3) = " + res);
    println("TotalValuesDistinct (Distinct operation): " + valuesDistinct.count());
  }

}
