package com.feel.recommend

import breeze.linalg.min
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable
import scala.util.Random.nextInt
import org.apache.spark.{SparkContext, SparkConf}
import org.elasticsearch.spark._

/**
 * Created by canoe on 9/17/15.
 */

case class JoinedGoalCardCandidates(user: String, candidates: Seq[String])

object RecommendCardBasedOnGoalJoined {

  private val REAL_USER_ID_BOUND = 1075
  private val GOAL_USER_NUMBER_THRESHOLD = 1000
  private val CARD_LIKED_BOTTOM_BOUND = 3
  private val CARD_LIKED_UP_BOUND = 30
  private val CANDIDATES_SIZE = 100

  def knuthShuffle[T](x: Array[T]) = {
    for (i <- (1 until x.length).reverse) {
      val j = nextInt(i + 1)
      val tmp = x(j)
      x(j) = x(i)
      x(i) = tmp
    }
    x
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf()

    conf.set("es.mapping.id", "user")
    conf.set("es.nodes", args(0))

    val sc = new SparkContext(conf)

    val recentlyActiveUser = sc.textFile(args(7))
      .map(x => x.split("\t"))
      .filter(_.length == 2)
      .map(x => (x(0), x(1)))

    val userGoal = sc.textFile(args(1)) // user, goal
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(0).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(0), x(1)))
      .join(recentlyActiveUser)
      .map(x => (x._2._1, x._1))

    val goalUserRecommend = userGoal
      .groupByKey()
      .flatMap(x => {
        val goalUserNumber = x._2.size
        val goalUsers = x._2.toArray
        if (x._2.size >= GOAL_USER_NUMBER_THRESHOLD) {
          val itNumber = min(goalUserNumber / GOAL_USER_NUMBER_THRESHOLD, 10)
          val result = new Array[(String, (String, String))](itNumber * GOAL_USER_NUMBER_THRESHOLD *
            GOAL_USER_NUMBER_THRESHOLD)
          for(it <- 0 until itNumber) {
            val sampledUsers = knuthShuffle(goalUsers).take(GOAL_USER_NUMBER_THRESHOLD)
            for(i <- 0 until GOAL_USER_NUMBER_THRESHOLD) {
              for(j <- 0 until GOAL_USER_NUMBER_THRESHOLD) {
                if (i != j)
                  result(it * GOAL_USER_NUMBER_THRESHOLD * GOAL_USER_NUMBER_THRESHOLD + i * GOAL_USER_NUMBER_THRESHOLD +
                    j) = (sampledUsers(i), (sampledUsers(j), x._1))
              }
            }
          }
          result.filter(_ != null).distinct.toSeq
        } else {
          val result = new Array[(String, (String, String))](goalUserNumber * goalUserNumber)
          for(i <- 0 until goalUserNumber) {
            for(j <- 0 until goalUserNumber) {
              if (i != j)
                result(i * goalUserNumber + j) = (goalUsers(i), (goalUsers(j), x._1))
            }
          }
          result.filter(_ != null).toSeq
        }
      })

    val cardLikedNumber = sc.textFile(args(2))
      .map(_.split("\t")) //user, card
      .filter(x => x.length == 2)
      .map(x => (x(1), 1))
      .reduceByKey((a, b) => a + b)
      .filter(x => x._2 >= CARD_LIKED_BOTTOM_BOUND && x._2 <= CARD_LIKED_UP_BOUND)

    val userCard = sc.textFile(args(3)) // user, card, type
      .map(_.split("\t"))
      .filter(x => x.length == 3 && x(0).toInt >= REAL_USER_ID_BOUND && x(2) == "card")
      .map(x => (x(1), x(0))) // card, user
      .join(cardLikedNumber) // filter
      .map(x => (x._2._1, (x._1, x._2._2))) //user, (card, likedNumber)

    val userFollowingSet = sc.textFile(args(4))
      .map(_.split("\t"))
      .filter(x => x.length == 2 && x(0).toInt >= REAL_USER_ID_BOUND && x(1).toInt >= REAL_USER_ID_BOUND)
      .map(x => (x(1), x(0)))
      .groupByKey()
      .map(x => (x._1, x._2.toSet))

    val result = goalUserRecommend
      .join(userCard) // recommended, ((user, goal), (recommendedUserCard, cardLikedNumber))
      .map(x => (x._2._1._1, (x._1, x._2._2._1, x._2._2._2, x._2._1._2)))
      //(user, recommendedUser, recommendedUserCard, cardLikedNumber, goal)
      .groupByKey()
      .map(x => {
      val cardCandidates = x._2
        .foldLeft(new mutable.HashMap[String, (String, Int, String)]())((userCardInfo, value) => {
        if (value._3 > userCardInfo.getOrElse(value._1, ("", -1, ""))._2)
          userCardInfo(value._1) = (value._2, value._3, value._4)
        userCardInfo
      }).toArray.map(y => (y._1, y._2._1, y._2._2, y._2._3))
      (x._1, cardCandidates)//user, {(recommendUser, recommendUserCard, cardLikedNumber, goal)}
    })
      .join(userFollowingSet) //user, ({cardInfo}, followingSet)
      .map(x => {
      val user = x._1
      val followingSet = x._2._2
      val cardCandidates = x._2._1.toSeq.filter(cardInfo => !followingSet(cardInfo._1))
        .sortWith(_._3 > _._3)
        .map(x => x._2 + ":" + x._4)
        .distinct
        .take(CANDIDATES_SIZE)
      (user, cardCandidates)
    })
    result.saveAsTextFile(args(5))
    result.map(x => JoinedGoalCardCandidates(x._1, x._2)).saveToEs(args(6))
  }
}
