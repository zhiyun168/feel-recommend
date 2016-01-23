package com.feel.statistics

import com.feel.utils.FeelUserRDD
import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/23/16.
  */
object UserInformation {

  def main(args: Array[String]) = {
    val sc = new SparkContext()
    val rdd = new FeelUserRDD(sc.textFile(args(0)), List("leader", "follower"), 0)
      .transform()
    rdd.map(x => (x(0))).saveAsTextFile(args(1))
  }
}
