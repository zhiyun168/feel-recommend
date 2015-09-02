package com.feel.statistics

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by canoe on 9/2/15.
 */


object UserClusterFeature {

  private val REAL_USER_ID_BOUND = 1075

  val featureMap = List(
    1 -> "everyDayLikeNumber",
    2 -> "everyDayLikedNumber",
    3 -> "everyDayCardNumber",
    4 -> "everyDayGoalNumber",
    5 -> "everyDayFollowNumber",
    6 -> "everyDayFollowedNumber",
    7 -> "userDaySinceRegister",
    8 -> "userGender").toMap

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userDayNumberSinceRegister = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => {
      (x(0) + "\t7:" + (System.currentTimeMillis() / 1000 - x(1).toLong) / (60 * 60 * 24))
    })

    val likeRDD = sc.textFile(args(1)) // user like
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)

    def userRDDTransform(rdd: RDD[Array[String]], index: Int, featureIndex: String) = {
      rdd
        .map(x => (x(index), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1 + "\t" + featureIndex + ":" + x._2))
    }
    val userLikeNumber = userRDDTransform(likeRDD, 0, "1")

    val cardRDD = sc.textFile(args(2)) // user, card, type
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)

    def cardRDDTransform(rdd: RDD[Array[String]], cardType: String, featureIndex: String) = {
      rdd
        .filter(x => x(2) == cardType)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1 + "\t" + featureIndex + ":" + x._2))
    }

    val userCardNumber = cardRDDTransform(cardRDD, "card", "3")
    val userGoalNumber = cardRDDTransform(cardRDD, "goal", "4")

    val followRDD = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)


    val userFollowingNumber = userRDDTransform(followRDD, 1, "5")
    val userFollowerNumber = userRDDTransform(followRDD, 0, "6")


    val cardOwner = cardRDD.map(x => (x(1), x(0)))
    val likedRDD = likeRDD.map(x => (x(1), x(0))) // like, user
      .join(cardOwner) // card, (user, owner)
      .map(x => Array(x._2._2)) // owner, card,

    val userLikedNumber = userRDDTransform(likedRDD, 0, "2")
    val userGender = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => x(0) + "\t8:" + {
      if (x(1) == "f") "0" else if (x(1) == "m") "1" else "2"
    })

    userGender.union(userLikeNumber).union(userLikedNumber).union(userFollowerNumber).union(userFollowingNumber)
      .union(userCardNumber).union(userGoalNumber).union(userDayNumberSinceRegister)
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      val norm = {
        val normTmp = x._2.map(_.split(":")).filter(_(0) == "7").head(1).toDouble
        if (normTmp == 0) 1 else normTmp
      }
      x._1 + "\t" + x._2.map(_.split(":")).map(x => {
        if (x(0) != "7") x(0) + ":" + (x(1).toDouble / norm).toString
        else x
      }).mkString("\t")
    }).saveAsTextFile(args(5))
  }
}
