package com.feel.recommend

import breeze.numerics.{abs, exp}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable
import scala.math.{log, max}

/**
 * Created by canoe on 8/7/15.
 */
object RecommendHotCardBasedOnKLDivergence {

  private val REAL_USER_ID_BOUND = 1075
  private var HOT_SCORE_THRESHOLD = 200D
  private val CANDIDATES_SIZE = 200
  private var NEW_USER_BOTTOM_LIKED_NUMBER = 3
  private var HALF_TIME = 8D

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

    val cardOwner = sc.textFile(args(1)) // 12 hour data
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

    NEW_USER_BOTTOM_LIKED_NUMBER = args(10).toInt

    val userCardHotScore = valueRDD
      .map(x => {
      /*x._2._2 match {
        case Some(historyCount) =>
          (x._1._1 + "\t" + x._2._1._1, (x._2._1._2 + 1D, log(historyCount + 1D))) // under test
        case None =>
         (x._1._1 + "\t" + x._2._1._1, (x._2._1._2 + 1D, NEW_USER_BOTTOM_LIKED_NUMBER.toDouble))
      }*/
      (x._1._1 + "\t" + x._2._1._1, (x._2._1._2 + 1D, NEW_USER_BOTTOM_LIKED_NUMBER.toDouble))

    }).groupByKey() // ((user, card), scoreTuple)
      .map(x => {
      val cardInfo = x._1
      val score = KLDivergence(x._2)
      (cardInfo, score)
    })

    val decay = abs(args(11).toDouble)
    HALF_TIME = args(12).toDouble

    val lambda = log(2) / HALF_TIME
    val decayFactor = exp(-lambda * decay)

    val decayedUserCardHotScore = userCardHotScore
      .map(x => {
      val tmp = x._1.split("\t")
      (tmp(0), (tmp(1), x._2))
    }).groupByKey()
    .map(x => {
      val user = x._1
      x._2.size match {
        case 1 => {
          val value = x._2.head
          (user, value._1, value._2, value._2 * decayFactor)
        }
        case _ => {
          val value = x._2.toArray.sortWith(_._2 > _._2).head
          (user, value._1, value._2, value._2 * decayFactor)
        }
      }
    })
    decayedUserCardHotScore.sortBy(_._3).saveAsTextFile(args(8))

    val cardSonTag = sc.textFile(args(4)) // 12 hour data
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), x(0))) // sonTag, card

    val cardParentTag = sc.textFile(args(5)) //
      .map(_.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(1), x(0))) // sonTag, parentTag
      .join(cardSonTag)
      .map(x => {
      (x._2._2, (x._2._1, x._1)) // card, (parentTag, sonTag)  - tag
    })

    HOT_SCORE_THRESHOLD = args(9).toDouble
    decayedUserCardHotScore
      .map(x => {
      (x._2, (x._1, x._4)) //card, (user, score)
    })
      .join(cardParentTag) // card, ((user, score), tag)
      .map(x => {
        val key = x._2._2._1 // parentTag
        val value = ((x._2._1._1, x._2._1._2, x._2._2._2), x._1) // ((user, score, sonTag), card)
      (key, value)
    })
      .groupByKey()
      .map(x => {
      val key = x._1
      val value = x._2.toSeq.sortWith(_._1._2 > _._1._2).distinct.take(CANDIDATES_SIZE).mkString("|")
      key + "|" + value
    })
      .saveAsTextFile (args(6))

    val userHistoryUpdatedMaxValueRDD = valueRDD.
      map(x =>
      x._2._2 match {
        case Some(historyCount) =>
          (x._1, max(x._2._1._2, historyCount))
        case None =>
          (x._1, max(x._2._1._2, NEW_USER_BOTTOM_LIKED_NUMBER))
      })
      .groupByKey()
      .map(x => {
      val key = x._1
      val value = x._2.foldLeft(0)((count, maxCount) => max(count, maxCount))
      (key, value)
    })

    userHistoryUpdatedMaxValueRDD.saveAsTextFile(args(7))

  }
}
