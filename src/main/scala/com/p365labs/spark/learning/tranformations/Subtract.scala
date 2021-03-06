package com.p365labs.spark.learning.tranformations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class Subtract {
  def testSubtract(sc: SparkContext): Unit = {
    val val1 = sc.parallelize(List("coca cola", "coffee", "Sprite", "Redbull"));
    val val2 = sc.parallelize(List("coca cola", "jagermeister", "vodka", "Redbull"));

    val val3 = val1.subtract(val2);
    /**
      * coffee
      * Sprite
      *
      */
    val3.collect().foreach(println);

    val val4 = val2.subtract(val1);
    /**
      * vodka
      * jagermeister
      */
    val4.collect().foreach(println);
  }
}
