package com.feel.issues

import com.feel.utils.FeelUserAggregatedRDD
import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/26/16.
  */
object UserShare {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    def f(x: String) = 1
    val rdd = new FeelUserAggregatedRDD(sc.textFile(args(0)), List("user", "share", "ts"),
      0, 1, f, 1420041600, 2)
    rdd.countUserInfo().saveAsTextFile(args(1))

  }
}
