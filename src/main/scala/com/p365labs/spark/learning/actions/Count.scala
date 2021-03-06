package com.p365labs.spark.learning.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by federicopanini on 06/06/16.
  */
class Count {
  /**
    *
    * @param sc
    */
  def testCount(sc: SparkContext): Unit = {
    val elements = sc.parallelize(List(1,2,3,3));

    val result = elements.count();

    println("");
    println("************************ ACTION COUNT");
    println("Actions : Count = " + result);
  }
}
