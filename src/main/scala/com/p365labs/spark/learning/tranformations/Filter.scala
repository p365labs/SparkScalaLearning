package com.p365labs.spark.learning.tranformations

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

class Filter {
  /**
    * This example show hot to implement filter transformation in Spark.
    * Filter return a new RDD (Resilient Distributed Dataset)
    *
    * @param sc SparkContext
    * @param inputFile
    * @param outputFile
    */
  def filterErrorLog(sc: SparkContext, inputFile: RDD[String], outputFile: String): Unit = {
    val errorRRD = inputFile.filter(line => line.contains("HTTP/1.1\" 500"));

    println("");
    println("************************ TRANSFORMATION FILTER");
    println("Total line numbers : " + inputFile.count());
    println("HTTP 500 error lines: " + errorRRD.count());
    println("Actions : values.filter(line => line.contains(\"HTTP/1.1\\\" 500\")");
    for (res <- errorRRD.takeSample(false, 3)) println("Actions : .top(3) = " + res);
    errorRRD.saveAsTextFile(outputFile);
  }

  /**
    * try to count and stats how many times googlebot crwaled the website.
    *
    * @param sc
    * @param inputFile
    */
  def countGoogleBot(sc: SparkContext, inputFile: RDD[String]): Unit = {
    val googlebot = inputFile.filter(line => line.contains("Googlebot/2.1"));

    println("Total GoogleBot GET : " + googlebot.count());
  }
}

