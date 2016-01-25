package com.feel.issues

import com.feel.utils.{TimeIssues, FeelUserRDD}
import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/25/16.
  */
object GoalInfo {

  def main(args: Array[String]) = {
    val sc = new SparkContext()

    val userGoalData = new FeelUserRDD(sc.textFile(args(0)), List("uid", "goalId", "ts"), 0)
      .transform()

    userGoalData.map(x => {
      (x(0), TimeIssues.tsToDate(x(2).toLong))
    }).distinct()
      .map(x => (x._1, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(_._2)
      .saveAsTextFile(args(1))
  }
}
