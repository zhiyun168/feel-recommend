package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.math.{log, max}

/**
 * Created by canoe on 8/7/15.
 */
object RecommendHotCardBasedOnKLDivergence {

  private val REAL_USER_ID_BOUND = 1075
  private var HOT_SCORE_THRESHOLD = 200D
  private val RDD_PARTITION_NUMBER = 100

  def KLDivergence(p: Iterable[(Double, Double)]) = {
    p.foldLeft(0D)((acc, value) => (acc + value._1 * log(value._1 / value._2)))
  }

  def parseHistoryHotScore(str: String) : ((String, String), Int) = {
    //(("user", "F"), 1) // (("user", "N") , 2)
    val features = str.replaceAll("[() ]", "").split(",")
    features.length match {
      case 3 => ((features(0), features(1)), features(2).toInt)
      case _ => null
    }
  }

  def main(args: Array[String]) = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val userHistoryMaxValueRDD = sc.textFile(args(0)) //((user, F / N), count)
      .map(parseHistoryHotScore(_))
      .filter(_ != null)

    val cardOwner = sc.textFile(args(1))
      .map(_.split("\t"))
      .filter(_.length == 3)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(2) == "card")
      .map(x => (x(1), x(0))) // card, owner

    val likedRDD = sc.textFile(args(2)) // last two hour data
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) //card, cUser
      .join(cardOwner) // card, (cUser, owner)
      .map(x => (x._2._2, (x._2._1, x._1))) // owner, (cUser, card)
      .groupByKey()

    val followerRDD = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND) // user, follower
      .map(x => (x(0), x(1))) // user, follower
      .groupByKey()
      .map(x => (x._1, x._2.toSet)) //user, followerSet

    val valueRDD = followerRDD
      .join(likedRDD) // user, (followerSet, Iterable(cUser, card))
      .flatMap(x => {
      val hashCount = new mutable.HashMap[(String, String, String), Int]()
      val followerSet = x._2._1
      x._2._2.map(y => { // (cUser, card)
        if (followerSet(y._1)) {
          val key = (x._1, y._2, "F")
          if (hashCount.get(key).isEmpty)
            hashCount(key) = 1
          else
            hashCount(key) += 1
        } else {
          val key = (x._1, y._2, "N")
          if (hashCount.get(key).isEmpty)
            hashCount(key) = 1
          else
            hashCount(key) += 1
        }
      })
      hashCount.toList.map(userLikeCardInfo =>
        ((userLikeCardInfo._1._1, userLikeCardInfo._1._3), (userLikeCardInfo._1._2, userLikeCardInfo._2)))
    }).leftOuterJoin(userHistoryMaxValueRDD) // ((user, F / N), ((card, count), historyCount))

    val userCardHotScore = valueRDD
      .map(x => {
      x._2._2 match {
        case Some(historyCount) =>
          (x._1._1 + "\t" + x._2._1._1, (x._2._1._2 + 1D, historyCount + 1D))
        case None =>
          (x._1._1 + "\t" + x._2._1._1, (x._2._1._2 + 1D, 1D))
      }
    }).groupByKey() // ((user, card), scoreTuple)
      .map(x => {
      val cardInfo = x._1
      val score = KLDivergence(x._2)
      (cardInfo, score)
    })

    HOT_SCORE_THRESHOLD = args(6).toDouble
    userCardHotScore.filter(_._2 > HOT_SCORE_THRESHOLD)
      .sortBy(x => x._2, true, RDD_PARTITION_NUMBER)
      .saveAsTextFile (args(4))

    val userHistoryUpdatedMaxValueRDD = valueRDD.
      map(x =>
      x._2._2 match {
        case Some(historyCount) =>
          (x._1, max(x._2._1._2, historyCount))
        case None =>
          (x._1, max(x._2._1._2, 1))
      })
      .groupByKey()
      .map(x => {
      val key = x._1
      val value = x._2.foldLeft(0)((count, maxCount) => max(count, maxCount))
      (key, value)
    })

    userHistoryUpdatedMaxValueRDD.saveAsTextFile(args(5))

  }
}
