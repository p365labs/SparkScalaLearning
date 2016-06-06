package com.p365labs.spark.learning

/**
  * Created by federicopanini on 06/06/16.
  */
class PassingFunctions(val query: String) {
  def isMatch(s: String): Boolean = {
    s.contains(query);
  }
}
