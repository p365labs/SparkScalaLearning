package com.p365labs.spark.learning.tranformations

import org.apache.spark.SparkContext

/**
  * If you want to make the class work it should extends Serializable.
  * We are using / passing a function to a map transformation, the class to work, and to avoid
  * a NotSerializableException would not work.
  *
  *
  * Created by federicopanini on 06/06/16.
  */
class Map extends Serializable {

  /**
    * in this example we show hot to output the mapresult with the collect() method.
    * Beware that collect() load the whole RDD into memory, so it's necessary that the objets fits
    * inside memory.
    *
    * @param sc
    */
  def testMap(sc: SparkContext): Unit ={
    val nums = sc.parallelize(List(1, 2, 3, 4));
    val result = nums.map(x => x * x);
    println(result.collect().mkString(","));
  }

  /**
    * this is a private function to be called inside a map function
    *
    * @param x
    * @return
    */
  private def greaterThan(x: String): String ={
    if (Integer.valueOf(x) > 1500) {
      return "0";
    } else {
      return x;
    }
  }

  /**
    * this method is used to test the functionality of calling an external function
    * passed direclty to the map tranformation.
    *
    * @param sc
    * @param integerFile
    */
  def testMapsecond(sc: SparkContext, integerFile: String): Unit = {
    val nums = sc.parallelize(List());
    val input = sc.textFile(integerFile);

    val result = input.map(greaterThan);

    println(result.collect().mkString(","));
  }
}
