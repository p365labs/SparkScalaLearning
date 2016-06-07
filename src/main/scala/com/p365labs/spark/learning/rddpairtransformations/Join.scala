package com.p365labs.spark.learning.rddpairtransformations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 07/06/16.
  */
class Join {
  def testJoin(sc: SparkContext): Unit = {
    val elements = sc.parallelize(
      List(
        (1,2),
        (3,4),
        (3,6)
      )
    );

    val other = sc.parallelize(List(
      (3, 9)
    )
    );

    val result = elements.join(other);

    println("");
    println("************************ PAIR RDD TRANSFORMATIONS JOIN");
    println("Actions : elements is List((1,2),(3,4),(3,6))");
    println("Actions : other is List((3,9))");
    println("Actions : elements.join(other)");
    for (res <- result) println("Actions : .join(other) = " + res);
  }

  def testJoinSecond(sc: SparkContext): Unit = {
    val person1Preference = sc.parallelize(List(
      ("Apple IIe", "Federico"),
      ("spritz", "Federico"),
      ("sunset", "Federico")
    ))

    val person2Preference = sc.parallelize(List(
      ("sunset", "Jackie"),
      ("spritz", "Jackie")
    ));

    val whatTheyLoveTogether = person1Preference.join(person2Preference);
    println("");
    println("************************ PAIR RDD TRANSFORMATIONS JOIN");
    println("Actions : elements is List((\"Apple IIe\", \"Federico\"),(\"spritz\", \"Federico\"), (\"sunset\", \"Federico\"))");
    println("Actions : other is List((\"sunset\", \"Jackie\"),(\"spritz\", \"Jackie\"))");
    println("Actions : elements.cogroup(other)");
    whatTheyLoveTogether.collect().foreach(row => println(row._1 + " " + row._2));
  }
}
