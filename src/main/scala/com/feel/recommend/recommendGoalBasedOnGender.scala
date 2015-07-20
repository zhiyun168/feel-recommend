package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._
import scala.util.Random.nextInt

/**
 * Created by canoe on 6/27/15.
 */

case class genderGoalRecommend(user: String, candidates: Seq[String])

object recommendGoalBasedOnGender {

  private val REAL_USER_ID_BOUND = 1075
  private var GOAL_THRESHOLD = 30
  private val RDD_PARTITION_SIZE = 10

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))
    val sc = new SparkContext(conf)

    val userGender = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 2) //user, gender
      .filter(_(0).toLong >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))

    GOAL_THRESHOLD = args(4).toInt

    val userGoal = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toLong >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) // user, goal

    val userGoalList = userGoal.reduceByKey((a, b) => a + "\t" + b)

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
      .map(x => (x._1, x._2.split("\t").toSeq.distinct))

    val result = userGender.map(x => (x._2, x._1))
      .join(genderGoal) // gender, user, rgoal
      .map(x => {
      val user = x._2._1
      val goalCandidates = x._2._2.map(y => (y, nextInt())).sortWith(_._2 > _._2).map(_._1)
      (user, goalCandidates)
    })
      .leftOuterJoin(userGoalList)
    .map(x => {
      val user = x._1
      x._2._2 match {
        case Some(joinedGoalList) => {
          val joinedGoalSet = joinedGoalList.split("\t").toSet
          val goalCandidates = x._2._1.filter(!joinedGoalSet(_))
          genderGoalRecommend(user, goalCandidates)
        }
        case None => {
          genderGoalRecommend(user, x._2._1)
        }
      }
    })
    result.saveToEs("recommendation/genderGoal")
    result.saveAsTextFile(args(3))
  }
}
