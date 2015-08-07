package com.feel.recommend

import breeze.linalg.max
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.math.log

/**
 * Created by canoe on 8/7/15.
 */
object RecommendHotCardBasedOnKLDivergence {

  private val REAL_USER_ID_BOUND = 1075

  def KLDivergence(p: Iterable[(Double, Double)]) = {
    p.foldLeft(0D)((acc, value) => (acc + value._1 / log(value._1 / value._2)))
  }

  def getFanScore(str: String) : ((String, String, String), Int) = {
    //(("user", "card", "F"), 1) // (("user", "card", "N") , 2)
    val features = str.replaceAll("[() ]", "").split(",")
    ((features(0), features(1), features(2)), features(3).toInt)
  }

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userHistoryMaxValueRDD = sc.textFile(args(0)) //(user, (max, ts), (max, ts))
      .map(getFanScore(_))

    val likedRDD = sc.textFile(args(1)) // last two hour data
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND) // owner, cUser, card
      .map(x => (x(0), (x(1), x(2))))
      .groupByKey()

    val followerRDD = sc.textFile(args(2))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND) // user, follower
      .map(x => (x(0), x(1))) // user, followerList
      .groupByKey()
      .map(x => (x._1, x._2.toSet))

    val valueRDD = followerRDD
      .join(likedRDD) // user, (followerList, (cUser, card))
      .flatMap(x => {
      val hashCount = new mutable.HashMap[(String, String, String), Int]()
      val followerSet = x._2._1.toSet
      x._2._2.map(y => { // (cUser, card)
        if (followerSet(y._1)) {
          val key = (x._1, y._2, "F")
          if (hashCount.get(key).isEmpty)
            hashCount(key) = 1
          else
            hashCount(key) += 1
        } else {
          val key = (x._1, y._1, "N")
          if (hashCount.get(key).isEmpty)
            hashCount(key) = 1
          else
            hashCount(key) += 1
        }
      })
      hashCount.toList
    }).join(userHistoryMaxValueRDD)

    val userCardHotScore = valueRDD
      .map(x => ((x._1._1, x._1._2), (x._2._1 + 1D , x._2._2 + 1D)))
      .groupByKey()
      .map(x => {
      val cardInfo = x._1
      val score = KLDivergence(x._2)
      (cardInfo, score)
    })
    userCardHotScore.saveAsTextFile(args(3))

    val userHistoryUpdatedMaxValueRDD = valueRDD.
      map(x => (x._1, max(x._2._1, x._2._2)))

    userHistoryMaxValueRDD.saveAsTextFile(args(4))

  }
}
