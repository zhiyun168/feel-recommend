package com.feel.issues

import java.text.SimpleDateFormat

import breeze.linalg.max
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by canoe on 10/27/15.
 */
object MaxGoalStreakUserInfo {

  private val DAY_SECONDS_NUMBER = 86400000

  def dayDiff(aTs: Long, bTs: Long) = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val aDate = format.format(aTs - DAY_SECONDS_NUMBER)
    val bDate = format.format(bTs)
    if (aDate == bDate) true else false
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userGoalData = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .map(x => ((x(0), x(1)), x(2))) //(user, goalId), ts

    val userMaxGoalStreak = userGoalData
      .groupByKey()
      .map(x => {
      val user = x._1._1
      val goal = x._1._2
      val tsList = x._2.map(_.toLong * 1000).toSeq.sortWith(_ > _)
      val result = tsList.foldLeft((0, 0, tsList.head)) ((valueTuple, ts) => {
        val maxStreak = valueTuple._2
        val previousTs = valueTuple._3
        val currentStreak = if (dayDiff(previousTs, ts)) {
          val previousStreak = valueTuple._1
          previousStreak + 1
        } else {
          1
        }
        (currentStreak, max(currentStreak, maxStreak), ts)
      })
      (user, (goal, result._2))
    }).groupByKey()
      .map(x => {
      val user = x._1
      val maxStreakInfo = x._2.toSeq.sortWith(_._2 > _._2).head._2
      (user, maxStreakInfo)
    })

    val maxStreakDistribution = userMaxGoalStreak.map(x => {
      (x._2, 1)
    }).reduceByKey((a, b) => a + b)
    .map(x => x._1 + "\t" + x._2)
    maxStreakDistribution.saveAsTextFile(args(1))

    //userMaxGoalStreak.saveAsTextFile(args(1))

    def userFeatureCount(rdd: RDD[String]) = {
      rdd.map(_.split("\t"))
        .filter(_.length == 2)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1, x._2.toString))
    }

    val userFollowing = userFeatureCount(sc.textFile(args(2)))
    val userLikeNumber = userFeatureCount(sc.textFile(args(3)))
    val userCommentNumber = userFeatureCount(sc.textFile(args(4)))
    val userCardNumber = userFeatureCount(sc.textFile(args(5)))

    val userMaxStreak = userMaxGoalStreak.map(x => (x._1, x._2)) //user, maxStreak
      .groupByKey()
      .map(x => {
      val user = x._1
      val maxStreak = x._2.toSeq.max
      (user, maxStreak.toString)
    })

    def maxStreakUserInfo(aRDD: RDD[(String, String)], bRDD: RDD[(String, String)], featureInfo: String) = {
      aRDD.join(bRDD)
      .map(x => (x._2, 1)) //maxStreak, featureNumber, userNumber
      .reduceByKey((a, b) => a + b)
      .map(x => featureInfo + "\t" + x._1._1 + "\t" + x._1._2 + "\t" + x._2)
    }

    maxStreakUserInfo(userMaxStreak, userFollowing, "following").saveAsTextFile(args(6))
    maxStreakUserInfo(userMaxStreak, userLikeNumber, "like").saveAsTextFile(args(7))
    maxStreakUserInfo(userMaxStreak, userCommentNumber, "comment").saveAsTextFile(args(8))
    maxStreakUserInfo(userMaxStreak, userCardNumber, "card").saveAsTextFile(args(9))
  }
}
