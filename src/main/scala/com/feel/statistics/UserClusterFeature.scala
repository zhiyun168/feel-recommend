package com.feel.statistics

import breeze.linalg.min
import breeze.numerics.{floor, log}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by canoe on 9/2/15.
 */


object UserClusterFeature {

  private val REAL_USER_ID_BOUND = 1075
  private val FEATURE_VALUE_RANGE = 7

  val featureMap = List(
    0 -> "everyDayLikeNumber",
    1 -> "everyDayLikedNumber",
    2 -> "everyDayCardNumber",
    3 -> "everyDayGoalNumber",
    4 -> "everyDayFollowNumber",
    5 -> "everyDayFollowedNumber",
    6 -> "userDaySinceRegister",
    7 -> "userGender").toMap


  def featureIndex(index: Int, value: Int) = {
    index * FEATURE_VALUE_RANGE + min(FEATURE_VALUE_RANGE, floor(log(value)).toInt)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userDayNumberSinceRegister = sc.textFile(args(0))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => {
      (x(0) + "\t" + featureIndex(6, ((System.currentTimeMillis() / 1000 - x(1).toLong) / (60 * 60 * 24)).toInt) +
        ":1")
    })

    val likeRDD = sc.textFile(args(1)) // user like
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)

    def userRDDTransform(rdd: RDD[Array[String]], index: Int, rawIndex: Int) = {
      rdd
        .map(x => (x(index), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1 + "\t" + featureIndex(rawIndex, x._2) + ":1"))
    }
    val userLikeNumber = userRDDTransform(likeRDD, 0, 0)

    val cardRDD = sc.textFile(args(2)) // user, card, type
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)

    def cardRDDTransform(rdd: RDD[Array[String]], cardType: String, rawIndex: Int) = {
      rdd
        .filter(x => x(2) == cardType)
        .map(x => (x(0), 1))
        .reduceByKey((a, b) => a + b)
        .map(x => (x._1 + "\t" + featureIndex(rawIndex, x._2) + ":1"))
    }

    val userCardNumber = cardRDDTransform(cardRDD, "card", 2)
    val userGoalNumber = cardRDDTransform(cardRDD, "goal", 3)

    val followRDD = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)


    val userFollowingNumber = userRDDTransform(followRDD, 1, 4)
    val userFollowerNumber = userRDDTransform(followRDD, 0, 5)


    val cardOwner = cardRDD.map(x => (x(1), x(0)))
    val likedRDD = likeRDD.map(x => (x(1), x(0))) // like, user
      .join(cardOwner) // card, (user, owner)
      .map(x => Array(x._2._2)) // owner, card,

    val userLikedNumber = userRDDTransform(likedRDD, 0, 1)
    val userGender = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(_(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => x(0) + "\t" + {
      if (x(1) == "f") "50:1" else if (x(1) == "m") "51:1" else "52:1" // norm
    })

    userGender.union(userLikeNumber).union(userLikedNumber).union(userFollowerNumber).union(userFollowingNumber)
      .union(userCardNumber).union(userGoalNumber).union(userDayNumberSinceRegister)
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .groupByKey()
      .map(x => {
      /*val norm = {
        val tmp = x._2.map(_.split(":")).filter(_(0) == "7")
        if (tmp.size == 0) -1
        else {
          val normTmp = tmp.head(1).toDouble
          if (normTmp == 0) 1 else normTmp
        }
      }*/
      /*x._1 + "\t" + x._2.map(_.split(":")).map(y => {
        if (y(0) != "7" && y(0) != "8") y(0) + ":" + (y(1).toDouble / norm).toString
        else y(0) + ":" + y(1)*/
      x._1 + "\t" + x._2.mkString("\t")
    }).saveAsTextFile(args(5))
  }
}
