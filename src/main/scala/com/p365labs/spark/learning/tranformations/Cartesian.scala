package com.p365labs.spark.learning.tranformations

import org.apache.spark.SparkContext

/**
  * Created by federicopanini on 06/06/16.
  */
class Cartesian {
  def testCartesian(sc: SparkContext): Unit = {
    val val1 = sc.parallelize(List("coca cola", "coffee", "Sprite", "Redbull"));
    val val2 = sc.parallelize(List("coca cola", "jagermeister", "vodka", "Redbull"));

    val val3 = val1.cartesian(val2);
    val val4 = val2.cartesian(val1);

    /**
      * (coca cola,coca cola)
      * (coca cola,coffee)
      * (coca cola,Sprite)
      * (coca cola,Redbull)
      * (jagermeister,coca cola)
      * (jagermeister,coffee)
      * (jagermeister,Sprite)
      * (jagermeister,Redbull)
      * (vodka,coca cola)
      * (vodka,coffee)
      * (vodka,Sprite)
      * (vodka,Redbull)
      * (Redbull,coca cola)
      * (Redbull,coffee)
      * (Redbull,Sprite)
      * (Redbull,Redbull)
      */

    println("");
    println("************************ TRANSFORMATION CARTESIAN");
    println("Actions : val1.cartesian(val2)  where elements is List(\"coca cola\", \"coffee\", \"Sprite\", \"Redbull\")");
    println("Actions : val2 is List(\"coca cola\", \"jagermeister\", \"vodka\", \"Redbull\")");
    val3.collect().foreach(println);


    /**
      * (coca cola,coca cola)
      * (coca cola,coffee)
      * (coca cola,Sprite)
      * (coca cola,Redbull)
      * (jagermeister,coca cola)
      * (jagermeister,coffee)
      * (jagermeister,Sprite)
      * (jagermeister,Redbull)
      * (vodka,coca cola)
      * (vodka,coffee)
      * (vodka,Sprite)
      * (vodka,Redbull)
      * (Redbull,coca cola)
      * (Redbull,coffee)
      * (Redbull,Sprite)
      * (Redbull,Redbull)
      */
    println("");
    println("************************ TRANSFORMATION CARTESIAN");
    println("Actions : val2.cartesian(val1)  where elements is List(\"coca cola\", \"coffee\", \"Sprite\", \"Redbull\")");
    println("Actions : val2 is List(\"coca cola\", \"jagermeister\", \"vodka\", \"Redbull\")");
    val4.collect().foreach(println);
  }
}
