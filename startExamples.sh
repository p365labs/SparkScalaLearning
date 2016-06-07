#!/bin/sh

$SPARK_HOME/bin/spark-submit --class "com.p365labs.spark.learning.TestRunner" --master local[4] out/artifacts/test/test.jar "file:///Users/federicopanini/projects/SparkScalaLearning/src/main/resources" "file:///Users/federicopanini/projects/SparkScalaLearning/out/output.txt" "file:///Users/federicopanini/projects/SparkScalaLearning/src/main/resources/integers.txt"
