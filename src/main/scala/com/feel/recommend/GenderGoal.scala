package com.feel.recommend

import org.apache.spark.SparkContext

/**
  * Created by canoe on 1/19/16.
  */
object GenderGoal {

  private val REAL_USER_ID_BOUND = 1075
  private var GOAL_THRESHOLD = 30
  private val RDD_PARTITION_SIZE = 10

  def main(args: Array[String]) = {

    val sc = new SparkContext()

    val userGender = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2) //user, gender
      .filter(_ (0).toLong >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))

    GOAL_THRESHOLD = args(3).toInt

    val userGoal = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toLong >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) // user, goal

    val genderGoal = userGoal
      .join(userGender) //user, goal, gender
      .map(x => (x._2._1 + "\t" + x._2._2, 1))
      .reduceByKey((a, b) => a + b)
      .map(x => {
        val tmp = x._1.split("\t")
        val (goalId, gender) = (tmp(0), tmp(1))
        (goalId, gender + "\t" + x._2.toString)
      }) //
      .groupByKey(RDD_PARTITION_SIZE)
      .map(x => (x._1, x._2.toSeq.map(_.split("\t")).map(x => (x(0), x(1).toInt)).sortWith(_._2 > _._2)))
      .filter(_._2.length >= 2)
      .map(x => {
        val genderInfo = (x._2(0)._1, x._2(0)._2) // gender, number
        (x._1, genderInfo) // goalId, genderInfo
      }).filter(_._2._2 > GOAL_THRESHOLD) //at least 10 people have joined the goal
      .map(x => (x._2._1, x._1)) //gender rgoal
      .reduceByKey((a, b) => a + "\t" + b)
      .map(x => (if (x._1 == "f") "0" else "1", x._2.split("\t").toSeq.distinct))

    genderGoal.saveAsTextFile(args(2))
  }
}
