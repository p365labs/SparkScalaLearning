package com.p365labs.spark.learning.actions

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by federicopanini on 06/06/16.
  */
class Collect {
  /**
    *
    * @param sc
    */
  def testCollect(sc: SparkContext): Unit = {

    val elements = sc.parallelize(List(1, 2, 3, 3));

    val result = elements.collect();

    println("");
    println("************************ ACTION COLLECT");
    println("Actions : Collect = " + result.mkString(","));
  }
}