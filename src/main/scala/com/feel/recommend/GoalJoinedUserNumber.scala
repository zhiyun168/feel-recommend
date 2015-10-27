package com.feel.issues

import org.apache.spark.{SparkContext, SparkConf}
import java.sql.DriverManager
import org.apache.spark.rdd.JdbcRDD


/**
 * Created by canoe on 10/27/15.
 */
object GoalJoinedUserNumber {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userGoalRDD = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)
      .map(x => (x._2, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => x._1 + "\t" + x._2)
    userGoalRDD.saveAsTextFile(args(1))
  }
}
