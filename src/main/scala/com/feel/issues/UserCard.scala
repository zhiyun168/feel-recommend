package com.feel.issues

import com.feel.utils.{FeelUserAggregatedRDD}
import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/25/16.
  */


object UserCard {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    def f(x: String) = 1
    val rdd = new FeelUserAggregatedRDD(sc.textFile(args(0)), List("user", "card"), 0, 1, f)
    rdd.countUserInfo().saveAsTextFile(args(1))

  }
}