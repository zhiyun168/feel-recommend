package com.feel.recommend

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.math.{log, max}

/**
 * Created by canoe on 8/7/15.
 */
object RecommendHotCardBasedOnKLDivergence {

  private val REAL_USER_ID_BOUND = 1075

  def KLDivergence(p: Iterable[(Double, Double)]) = {
    p.foldLeft(0D)((acc, value) => (acc + value._1 / log(value._1 / value._2)))
  }

  def parseHistoryHotScore(str: String) : ((String, String), Int) = {
    //(("user", "F"), 1) // (("user", "N") , 2)
    val features = str.replaceAll("[() ]", "").split(",")
    features.length match {
      case 3 => ((features(0), features(1)), features(3).toInt)
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
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0))) // card, owner

    val likedRDD = sc.textFile(args(2)) // last two hour data
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1))) //card, cUser
      .join(cardOwner) // card, (cUser, owner)
      .map(x => (x._2._2, (x._1, x._2._1)))
      .groupByKey()

    val followerRDD = sc.textFile(args(3))
      .map(_.split("\t"))
      .filter(_.length == 2)
      .filter(x => x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND) // user, follower
      .map(x => (x(0), x(1))) // user, followerList
      .groupByKey()
      .map(x => (x._1, x._2.toSet))

    val valueRDD = followerRDD
      .join(likedRDD) // user, (followerList, Iterable(cUser, card))
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
    userCardHotScore.saveAsTextFile(args(4))

    val userHistoryUpdatedMaxValueRDD = valueRDD.
      map(x =>
      x._2._2 match {
        case Some(historyCount) =>
          (x._1, max(x._2._1._2, historyCount))
        case None =>
          (x._1, max(x._2._1._2, 1))
      })

    userHistoryUpdatedMaxValueRDD.saveAsTextFile(args(5))

  }
}
