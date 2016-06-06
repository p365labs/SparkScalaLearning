package com.p365labs.spark.learning


import com.p365labs.spark.learning.actions._
import com.p365labs.spark.learning.tranformations._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by federicopanini on 06/06/16.
  */
object TestRunner {
  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Filter");
      val sc = new SparkContext(conf);

      val inputFile = args(0) + "/nginx.access.log";
      val input = sc.textFile(inputFile);
      val integersFile = args(2);

      val rdd1 = input.filter(line => line.contains("31/May/2016"));
      val rdd2 = input.filter(line => line.contains("06/Jun/2016"));


      /**
        * instantiate Examples for Transformation
        */
      val filter = new Filter();
      val union = new Union();
      val map = new Map();
      val flatMap = new FlatMap();
      val distinct = new Distinct();
      val intersection = new Intersection();
      val subtract = new Subtract();
      val cartesian = new Cartesian();
      val collect = new Collect();
      val count = new Count();
      val countByValue = new CountByValue();
      val take = new Take();
      val takeOrdered = new TakeOrdered();
      val takeSample = new TakeSample();
      val top = new Top();


    /**
      * instantiate Examples for Transformation
      */
      val reduce = new Reduce();

      /**
        * run Examples
        */
      if (args.length > 0 ) {
        //filter
        filter.filterErrorLog(sc, input, args(1));
        filter.countGoogleBot(sc, input);

        //Union
        union.unionBetweenTwoRDD(rdd1, rdd2);

        //Map
        map.testMap(sc);
        map.testMapsecond(sc, integersFile);

        //FlatMap
        flatMap.testFlatMap(sc);
        flatMap.testFlatMap2(sc);

        //Distinct
        distinct.testDistinct(sc);

        //Intersection
        intersection.testIntersection(sc);

        //Subtract
        subtract.testSubtract(sc);

        //Cartesian
        cartesian.testCartesian(sc);

        //Reduce
        reduce.testReduce(sc);

        //Collect
        collect.testCollect(sc);

        //Count
        count.testCount(sc);

        //CountByValye
        countByValue.testCountByValue(sc);

        //Take
        take.testTake(sc);

        //TakeOrdered
        takeOrdered.testTakeOrdered(sc);

        //TakeSample
        takeSample.testTakeSample(sc);

        //Top
        top.testTop(sc);
      } else {
        println("********* You need to pass paramenter to this command in order to work properly");
      }
  }
}
