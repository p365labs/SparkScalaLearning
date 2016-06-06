package com.p365labs.spark.learning.tranformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by federicopanini on 06/06/16.
  */
class Union {
  def unionBetweenTwoRDD(rdd1: RDD[String], rdd2: RDD[String]): Unit = {

    val rdd3 = rdd1.union(rdd2);

    println("Union between RDD1 and RDD2 : total values --> " + rdd3.count());
  }
}
