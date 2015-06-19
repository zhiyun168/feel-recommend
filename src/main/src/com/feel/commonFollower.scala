package com.feel

/**
 * Created by canoe on 6/17/15.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

object commonFollower {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf)
    sc.textFile(args(0))
      .map(line => line.split("\t"))
      .filter(_.length == 3)
      .filter(_(0) != 1)
      .filter(_(1) != 1)
      .map(x => (x(1) + "\t#\t", x(0))) // to Tuple2
      .reduceByKey((a , b) => a + "\t" + b) //action
      .flatMap(line => {
      val followerArray = line._2.toString().replaceAll("[(),]", "").split("\t") //
      val result = new Array[String](followerArray.length * followerArray.length + 10)
      for (i <- 0 to followerArray.length - 1) {
        for (j <- 0 to followerArray.length - 1) {
          if (i != j)
            result(i * followerArray.length + j) = followerArray(i) + "\t" + followerArray(j)
        }
      }
      result.toList.filter(_ != null)
    }).map(x => (x, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => {
      val pair = x._1.split("\t")
      pair(0) + "\t" + x._2.toString + "\t" + pair(1)
    })
      .saveAsTextFile(args(1))
  }
}