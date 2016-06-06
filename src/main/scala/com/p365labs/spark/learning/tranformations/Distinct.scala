package com.p365labs.spark.learning.tranformations

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by federicopanini on 06/06/16.
  */
class Distinct {
  def testDistinct(sc: SparkContext): Unit = {
    val values = sc.parallelize(List(1,2,3,4,5,6,7,5,4,4,4,4,3,2,2,2,2,2,2,1));

    println("TotalValues (Distinct operation): " + values.count());

    val valuesDistinct = values.distinct();

    println("TotalValuesDistinct (Distinct operation): " + valuesDistinct.count());
  }

}
